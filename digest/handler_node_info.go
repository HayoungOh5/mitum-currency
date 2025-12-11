package digest

import (
	"context"
	"encoding/base64"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	isaacnetwork "github.com/ProtoconNet/mitum2/isaac/network"
	"github.com/ProtoconNet/mitum2/network/quicmemberlist"
	"github.com/ProtoconNet/mitum2/network/quicstream"
	quicstreamheader "github.com/ProtoconNet/mitum2/network/quicstream/header"
	"github.com/ProtoconNet/mitum2/util/encoder"
	"github.com/pkg/errors"
)

func (hd *Handlers) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	self := ParseBoolQuery(r.URL.Query().Get("self"))

	cacheKey := CacheKeyPath(r)
	if err := LoadFromCache(hd.cache, cacheKey, w); err == nil {
		return
	}

	if v, err, shared := hd.rg.Do(cacheKey, func() (interface{}, error) {
		i, err := hd.handleNodeInfoInGroup(self)

		return i, err
	}); err != nil {
		hd.Log().Err(err).Msg("get node info")

		HTTP2HandleError(w, err)
	} else {
		HTTP2WriteHalBytes(hd.enc, w, v.([]byte), http.StatusOK)

		if !shared {
			HTTP2WriteCache(w, cacheKey, hd.expireShortLived)
		}
	}
}

type nodeInfoResult struct {
	info *isaacnetwork.NodeInfo
	conn quicstream.ConnInfo
}

func (hd *Handlers) collectNodeInfo(self bool) ([]nodeInfoResult, error) {
	client := hd.baseClient
	memberList := hd.memberList
	nodeList := hd.staticNodeList

	if client == nil {
		return nil, errors.New("network client is not initialized")
	}

	connInfo := make(map[string]quicstream.ConnInfo)

	if !self {
		// memberList가 nil이 아닐 때만 실행 (방어 코드)
		if memberList != nil {
			memberList.Members(func(node quicmemberlist.Member) bool {
				connInfo[node.ConnInfo().String()] = node.ConnInfo()
				return true
			})
		}
		for _, c := range nodeList {
			connInfo[c.String()] = c
		}
	} else {
		connInfo[hd.node.String()] = hd.node
	}

	results := make([]nodeInfoResult, 0, len(connInfo))

	for key := range connInfo {
		nodeInfo, err := NodeInfo(client, connInfo[key])
		if err != nil {
			continue
		}

		results = append(results, nodeInfoResult{
			info: nodeInfo,
			conn: connInfo[key],
		})
	}

	return results, nil
}

func (hd *Handlers) handleNodeInfoInGroup(self bool) (interface{}, error) {
	results, err := hd.collectNodeInfo(self)
	if err != nil {
		return nil, err
	}

	nodeInfoList := make([]isaacnetwork.NodeInfo, 0, len(results))
	for i := range results {
		if results[i].info == nil {
			continue
		}

		nodeInfoList = append(nodeInfoList, *results[i].info)
	}

	if i, err := hd.buildNodeInfoHal(nodeInfoList); err != nil {
		return nil, err
	} else {
		return hd.enc.Marshal(i)
	}
}

func (hd *Handlers) handleNodeInfoProm(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	self := ParseBoolQuery(r.URL.Query().Get("self"))

	results, err := hd.collectNodeInfo(self)
	if err != nil {
		hd.Log().Err(err).Msg("get node info for prometheus")
		HTTP2HandleError(w, err)

		return
	}

	var b strings.Builder
	writePromNodeInfo(&b, results)

	w.Header().Set("Content-Type", PrometheusTextMimetype)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
}

func (hd *Handlers) buildNodeInfoHal(ni []isaacnetwork.NodeInfo) (Hal, error) {
	var hal Hal = NewBaseHal(ni, NewHalLink(HandlerPathNodeInfo, nil))

	return hal, nil
}

func writePromNodeInfo(b *strings.Builder, results []nodeInfoResult) {
	headersWritten := map[string]bool{}

	if len(results) == 0 {
		b.WriteString("# No node info available\n")

		return
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].conn.String() < results[j].conn.String()
	})

	for i := range results {
		info := results[i].info
		if info == nil {
			continue
		}

		baseLabels := map[string]string{
			"node": info.ConnInfo(),
		}

		if addr := info.Address(); addr != nil {
			baseLabels["address"] = addr.String()
		}

		if v := info.Version(); !v.IsEmpty() {
			baseLabels["version"] = v.String()
		}

		if ni := info.JSONMarshaler().NetworkID; len(ni.Bytes()) > 0 {
			baseLabels["network_id"] = base64.StdEncoding.EncodeToString(ni.Bytes())
		}

		cloneLabels := func(extra map[string]string) map[string]string {
			labels := make(map[string]string, len(baseLabels))
			for k, v := range baseLabels {
				labels[k] = v
			}
			for k, v := range extra {
				labels[k] = v
			}

			return labels
		}

		startedAt := info.StartedAt()
		if !startedAt.IsZero() {
			writePromSample(
				b,
				"mitum_node_info_started_timestamp_seconds",
				cloneLabels(nil),
				strconv.FormatFloat(float64(startedAt.UnixNano())/1e9, 'f', -1, 64),
				headersWritten,
			)
		}

		writePromSample(
			b,
			"mitum_node_info_suffrage_height",
			cloneLabels(nil),
			strconv.FormatInt(info.SuffrageHeight().Int64(), 10),
			headersWritten,
		)

		writePromSample(
			b,
			"mitum_node_info_consensus_members",
			cloneLabels(nil),
			strconv.FormatInt(int64(len(info.ConsensusNodes())), 10),
			headersWritten,
		)

		if manifest := info.LastManifest(); manifest != nil {
			writePromSample(
				b,
				"mitum_node_info_last_manifest_height",
				cloneLabels(nil),
				strconv.FormatInt(manifest.Height().Int64(), 10),
				headersWritten,
			)

			if !manifest.ProposedAt().IsZero() {
				writePromSample(
					b,
					"mitum_node_info_last_manifest_proposed_timestamp_seconds",
					cloneLabels(nil),
					strconv.FormatFloat(float64(manifest.ProposedAt().UnixNano())/1e9, 'f', -1, 64),
					headersWritten,
				)
			}
		}

		if policy := info.NetworkPolicy(); policy != nil {
			writePromSample(
				b,
				"mitum_node_info_network_policy_max_operations_in_proposal",
				cloneLabels(nil),
				strconv.FormatUint(policy.MaxOperationsInProposal(), 10),
				headersWritten,
			)

			writePromSample(
				b,
				"mitum_node_info_network_policy_max_suffrage_size",
				cloneLabels(nil),
				strconv.FormatUint(policy.MaxSuffrageSize(), 10),
				headersWritten,
			)

			writePromSample(
				b,
				"mitum_node_info_network_policy_suffrage_candidate_lifespan",
				cloneLabels(nil),
				strconv.FormatInt(policy.SuffrageCandidateLifespan().Int64(), 10),
				headersWritten,
			)

			writePromSample(
				b,
				"mitum_node_info_network_policy_suffrage_expel_lifespan",
				cloneLabels(nil),
				strconv.FormatInt(policy.SuffrageExpelLifespan().Int64(), 10),
				headersWritten,
			)

			var emptyProposalValue string
			if policy.EmptyProposalNoBlock() {
				emptyProposalValue = "1"
			} else {
				emptyProposalValue = "0"
			}

			writePromSample(
				b,
				"mitum_node_info_network_policy_empty_proposal_no_block",
				cloneLabels(nil),
				emptyProposalValue,
				headersWritten,
			)
		}

		lastVote := info.LastVote()
		if !lastVote.Point.IsZero() {
			writePromSample(
				b,
				"mitum_node_info_last_vote_height",
				cloneLabels(nil),
				strconv.FormatInt(lastVote.Point.Height().Int64(), 10),
				headersWritten,
			)

			writePromSample(
				b,
				"mitum_node_info_last_vote_round",
				cloneLabels(nil),
				strconv.FormatUint(lastVote.Point.Round().Uint64(), 10),
				headersWritten,
			)
		}

		writePromSample(
			b,
			"mitum_node_info_last_vote_state",
			cloneLabels(map[string]string{
				"stage":  lastVote.Point.Stage().String(),
				"result": lastVote.Result.String(),
			}),
			"1",
			headersWritten,
		)
	}
}

func NodeInfo(client *isaacnetwork.BaseClient, connInfo quicstream.ConnInfo) (*isaacnetwork.NodeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	stream, _, err := client.Dial(ctx, connInfo)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = client.Close()
	}()

	header := isaacnetwork.NewNodeInfoRequestHeader()

	var nodeInfo *isaacnetwork.NodeInfo
	err = stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		var enc encoder.Encoder

		switch rEnc, rh, err := broker.ReadResponseHead(ctx); {
		case err != nil:
			return err
		case !rh.OK():
			return errors.Errorf("Not ok")
		case rh.Err() != nil:
			return rh.Err()
		default:
			enc = rEnc
		}

		switch bodyType, bodyLength, r, err := broker.ReadBodyErr(ctx); {
		case err != nil:
			return err
		case bodyType == quicstreamheader.EmptyBodyType,
			bodyType == quicstreamheader.FixedLengthBodyType && bodyLength < 1:
			return errors.Errorf("Empty body")
		default:
			var v interface{}
			if err := enc.StreamDecoder(r).Decode(&v); err != nil {
				return err
			}

			b, err := enc.Marshal(v)
			if err != nil {
				return err
			}

			h, err := enc.Decode(b)
			if err != nil {
				return err
			}

			ni, ok := h.(isaacnetwork.NodeInfo)
			if !ok {
				return errors.Errorf("expected isaacnetwork.NodeInfo, not %T", v)
			}

			nodeInfo = &ni

			return nil
		}
	})
	if err != nil {
		return nil, err
	}

	return nodeInfo, nil
}
