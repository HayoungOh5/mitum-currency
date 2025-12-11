package digest

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/ProtoconNet/mitum-currency/v3/digest/network"
	dutil "github.com/ProtoconNet/mitum-currency/v3/digest/util"
	"github.com/ProtoconNet/mitum2/base"
	isaacnetwork "github.com/ProtoconNet/mitum2/isaac/network"
	"github.com/ProtoconNet/mitum2/network/quicmemberlist"
	"github.com/ProtoconNet/mitum2/network/quicstream"
	"github.com/ProtoconNet/mitum2/util"
	"github.com/ProtoconNet/mitum2/util/encoder"
	"github.com/ProtoconNet/mitum2/util/logging"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"golang.org/x/net/http2"
)

type RequestWrapper struct {
	//Response *http.ResponseWriter
	body *bytes.Buffer
}

type HTTP2Server struct {
	sync.RWMutex
	*logging.Logging
	*util.ContextDaemon
	bind             string
	host             string
	networkID        base.NetworkID
	queue            chan RequestWrapper
	srv              *http.Server
	idleTimeout      time.Duration
	activeTimeout    time.Duration
	keepAliveTimeout time.Duration
	router           *mux.Router
	// client           func() (*isaacnetwork.BaseClient, *quicmemberlist.Memberlist, error)
	// client func() (*isaacnetwork.BaseClient, *quicmemberlist.Memberlist, []quicstream.ConnInfo, error)
	baseClient     *isaacnetwork.BaseClient
	memberList     *quicmemberlist.Memberlist
	staticNodeList []quicstream.ConnInfo
	encs           *encoder.Encoders
}

func NewHTTP2Server(
	bind, host string, certs []tls.Certificate, encs *encoder.Encoders, networkID base.NetworkID,
) (*HTTP2Server, error) {
	if err := dutil.CheckBindIsOpen("tcp", bind, time.Second*1); err != nil {
		return nil, errors.Wrap(err, "open digest server")
	}

	idleTimeout := time.Second * 10

	r := mux.NewRouter()
	//r.Use(RateLimiter(5, 20))

	sv := &HTTP2Server{
		Logging: logging.NewLogging(func(c zerolog.Context) zerolog.Context {
			return c.Str("module", "http2-server")
		}),
		bind:             bind,
		host:             host,
		networkID:        networkID,
		queue:            make(chan RequestWrapper, 100),
		idleTimeout:      idleTimeout,     // TODO can be configurable
		activeTimeout:    time.Minute * 1, // TODO can be configurable
		keepAliveTimeout: time.Minute * 1, // TODO can be configurable
		router:           r,
		encs:             encs,
	}

	srv, err := newHTTP2Server(sv, certs)
	if err != nil {
		return nil, err
	}
	sv.srv = srv

	sv.ContextDaemon = util.NewContextDaemon(sv.start)

	return sv, nil
}

func newHTTP2Server(sv *HTTP2Server, certs []tls.Certificate) (*http.Server, error) {
	srv := &http.Server{
		Addr:         sv.bind,
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Minute * 1,
		IdleTimeout:  sv.idleTimeout,
		TLSConfig: &tls.Config{
			Certificates: certs,
			MinVersion:   tls.VersionTLS12,
		},
		// ErrorLog:  // TODO connect with http logging
		Handler: network.HTTPLogHandler(sv.router, sv.Log()),
	}
	if err := http2.ConfigureServer(srv, &http2.Server{
		NewWriteScheduler: func() http2.WriteScheduler {
			return http2.NewPriorityWriteScheduler(nil)
		},
	}); err != nil {
		return nil, err
	}
	return srv, nil
}

func (sv *HTTP2Server) Initialize() error {
	if ln, err := net.Listen("tcp", sv.bind); err != nil {
		return err
	} else if err := ln.Close(); err != nil {
		return err
	}

	root := sv.router.Name("root")
	root.Path("/").HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		},
	)

	return nil
}

/*
func (sv *HTTP2Server) SetLogging(l *logging.Logging) *logging.Logging {
	_ = sv.ContextDaemon.SetLogging(l)

	return sv.Logging.SetLogging(l)
}
*/

func (sv *HTTP2Server) Router() *mux.Router {
	sv.RLock()
	defer sv.RUnlock()

	return sv.router
}

func (sv *HTTP2Server) SetEncoder(encs *encoder.Encoders) {
	sv.Lock()
	defer sv.Unlock()

	sv.encs = encs
}

func (sv *HTTP2Server) SetRouter(router *mux.Router) {
	sv.Lock()
	defer sv.Unlock()

	sv.router = router

	sv.srv.Handler = router
}

func (sv *HTTP2Server) start(ctx context.Context) error {
	ln, err := net.Listen("tcp", sv.bind)
	if err != nil {
		return err
	}
	sv.srv.Handler = network.HTTPLogHandler(sv.srv.Handler, sv.Log())

	var listener net.Listener = tcpKeepAliveListener{
		TCPListener:      ln.(*net.TCPListener),
		keepAliveTimeout: sv.keepAliveTimeout,
	}

	if len(sv.srv.TLSConfig.Certificates) > 0 {
		listener = tls.NewListener(listener, sv.srv.TLSConfig)
	}

	errchan := make(chan error)
	sv.srv.ConnState = sv.idleTimeoutHook()
	go func() {
		errchan <- sv.srv.Serve(listener)
	}()

	go func() {
		for {
			select {
			case req := <-sv.queue:
				go sv.HandleRequest(req)
			}
		}
	}()

	select {
	case err := <-errchan:
		if err != nil && errors.Is(err, http.ErrServerClosed) {
			sv.Log().Debug().Msg("server closed")

			return nil
		}

		sv.Log().Error().Err(err).Msg("something wrong")

		return err
	case <-ctx.Done():
		nctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		return sv.srv.Shutdown(nctx) // nolint:contextcheck
	default:
		return nil
	}
}

func (sv *HTTP2Server) idleTimeoutHook() func(net.Conn, http.ConnState) {
	var mu sync.Mutex
	m := map[net.Conn]*time.Timer{}
	return func(c net.Conn, cs http.ConnState) {
		mu.Lock()
		defer mu.Unlock()
		if t, ok := m[c]; ok {
			delete(m, c)
			t.Stop()
		}
		var d time.Duration
		switch cs {
		case http.StateIdle:
			d = sv.idleTimeout
		case http.StateNew, http.StateActive:
			d = sv.activeTimeout
		default:
			return
		}
		m[c] = time.AfterFunc(d, func() {
			sv.Log().Debug().
				Dur("idle-timeout", d).
				Str("remote", c.RemoteAddr().String()).
				Msg("closing idle conn after timeout")

			go func() {
				_ = c.Close()
			}()
		})
	}
}

func (sv *HTTP2Server) Queue() chan RequestWrapper {
	return sv.queue
}

func (sv *HTTP2Server) HandleRequest(wrapper RequestWrapper) {
	var v json.RawMessage
	if err := json.Unmarshal(wrapper.body.Bytes(), &v); err != nil {
		return
	} else if hinter, err := sv.encs.JSON().Decode(wrapper.body.Bytes()); err != nil {
		return
	} else if err := sv.sendItem(hinter); err != nil {
		return
	}
}

func (sv *HTTP2Server) sendItem(v interface{}) error {
	//switch t := v.(type) {
	//case base.Operation:
	//	if err := t.IsValid(sv.networkID); err != nil {
	//		return nil, err
	//	}
	//default:
	//	return nil, errors.Errorf("unsupported message type, %T", v)
	//}

	return sv.sendOperation(v)
}

func (sv *HTTP2Server) sendOperation(v interface{}) error {
	op, ok := v.(base.Operation)
	if !ok {
		return errors.Errorf("expected Operation, not %T", v)
	}

	sv.RLock()
	client := sv.baseClient
	memberList := sv.memberList
	nodeList := sv.staticNodeList
	sv.RUnlock()

	if client == nil {
		return errors.New("network client is not initialized")
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	connInfo := make(map[string]quicstream.ConnInfo)

	if memberList != nil {
		memberList.Members(func(node quicmemberlist.Member) bool {
			connInfo[node.ConnInfo().String()] = node.ConnInfo()
			return true
		})
	}

	for _, c := range nodeList {
		connInfo[c.String()] = c
	}

	errCh := make(chan error, len(connInfo))

	for _, ci := range connInfo {
		wg.Add(1)
		go func(node quicstream.ConnInfo) {
			defer wg.Done()
			// Reuses an already connected client (handshake omitted)
			_, err := client.SendOperation(ctx, node, op)
			if err != nil {
				errCh <- err
			}
		}(ci)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func (sv *HTTP2Server) buildHal(op base.Operation) (Hal, error) {
	var hal Hal = NewBaseHal(op, HalLink{})

	return hal, nil
}

func (sv *HTTP2Server) SetNetworkClient(
	client *isaacnetwork.BaseClient,
	memberList *quicmemberlist.Memberlist,
	nodeList []quicstream.ConnInfo,
) *HTTP2Server {
	sv.Lock()
	defer sv.Unlock()

	sv.baseClient = client
	sv.memberList = memberList
	sv.staticNodeList = nodeList

	return sv
}

type tcpKeepAliveListener struct {
	*net.TCPListener
	keepAliveTimeout time.Duration
}

func (ln tcpKeepAliveListener) Accept() (net.Conn, error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return nil, err
	}
	if err := tc.SetKeepAlive(true); err != nil {
		return nil, err
	}

	if err := tc.SetKeepAlivePeriod(ln.keepAliveTimeout); err != nil {
		return nil, err
	}

	return tc, nil
}
