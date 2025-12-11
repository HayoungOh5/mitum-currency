package digest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ProtoconNet/mitum-currency/v3/common"
	"github.com/ProtoconNet/mitum2/base"
	"github.com/ProtoconNet/mitum2/network/quicmemberlist"
	"github.com/ProtoconNet/mitum2/network/quicstream"
	"github.com/pkg/errors"
)

func (hd *Handlers) handleQueueSend(w http.ResponseWriter, r *http.Request) {
	body := &bytes.Buffer{}
	if _, err := io.Copy(body, r.Body); err != nil {
		HTTP2ProblemWithError(w, err, http.StatusInternalServerError)
		return
	}
	var req = RequestWrapper{body: body}
	hd.queue <- req
	HTTP2WriteHal(hd.enc, w, NewBaseHal("Send operation successfully", HalLink{}), http.StatusOK)
}

func (hd *Handlers) handleSend(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body := &bytes.Buffer{}
	defer body.Reset()
	if _, err := io.Copy(body, r.Body); err != nil {
		HTTP2ProblemWithError(w, err, http.StatusInternalServerError)
		return
	}

	var hal Hal
	var v json.RawMessage
	if err := json.Unmarshal(body.Bytes(), &v); err != nil {
		HTTP2ProblemWithError(w, common.ErrDecodeJson.Wrap(err), http.StatusBadRequest)
		return
	} else if hinter, err := hd.enc.Decode(body.Bytes()); err != nil {
		nerr := err
		if !errors.Is(err, common.ErrDecodeJson) {
			nerr = common.ErrDecodeJson.Wrap(err)
		}
		HTTP2ProblemWithError(w, nerr, http.StatusBadRequest)
		return
	} else if h, err := hd.sendItem(hinter); err != nil {
		HTTP2ProblemWithError(w, err, http.StatusBadRequest)
		return
	} else {
		hal = h
	}
	HTTP2WriteHal(hd.enc, w, hal, http.StatusOK)
}

func (hd *Handlers) sendItem(v interface{}) (Hal, error) {
	return hd.sendOperation(v)
}

func (hd *Handlers) sendOperation(v interface{}) (Hal, error) {
	op, ok := v.(base.Operation)
	if !ok {
		return nil, errors.Errorf("expected Operation, not %T", v)
	}

	client := hd.baseClient
	memberList := hd.memberList
	nodeList := hd.staticNodeList

	if client == nil {
		return nil, errors.New("network client is not initialized")
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	connInfo := make(map[string]quicstream.ConnInfo)
	memberList.Members(func(node quicmemberlist.Member) bool {
		connInfo[node.ConnInfo().String()] = node.ConnInfo()
		return true
	})
	for _, c := range nodeList {
		connInfo[c.String()] = c
	}

	//sent, err := client.SendOperation(ctx, nodeList[0], op)
	//if err != nil {
	//	return nil, err
	//} else if !sent {
	//	return nil, errors.Errorf("failed to send operation")
	//}

	errCh := make(chan error, len(connInfo))
	sentCh := make(chan bool, len(connInfo))
	for _, ci := range connInfo {
		wg.Add(1)
		go func(node quicstream.ConnInfo) {
			defer wg.Done()

			sent, err := client.SendOperation(ctx, node, op)
			if err != nil {
				errCh <- err
			}
			if sent {
				sentCh <- sent
			}
		}(ci)
	}
	go func() {
		wg.Wait()
		close(errCh)
		close(sentCh)
	}()

	var errList []error
	var sentList []bool
loop:
	for {
		select {
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
			} else if err != nil {
				errList = append(errList, err)
			}
		case sent, ok := <-sentCh:
			if !ok {
				sentCh = nil
			} else if sent {
				sentList = append(sentList, sent)
			}
		}

		if errCh == nil && sentCh == nil {
			break loop
		}
	}

	if len(sentList) < 1 {
		if len(errList) > 0 {
			return nil, errList[0]
		} else {
			return nil, errors.Errorf("Failed to send operation to node")
		}
	}

	return hd.buildSealHal(op)
}

func (hd *Handlers) buildSealHal(op base.Operation) (Hal, error) {
	var hal Hal = NewBaseHal(op, HalLink{})

	return hal, nil
}
