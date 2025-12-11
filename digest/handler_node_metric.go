package digest

import (
	"context"
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

func (hd *Handlers) handleNodeMetric(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	self := ParseBoolQuery(r.URL.Query().Get("self"))

	cacheKey := CacheKeyPath(r)
	if err := LoadFromCache(hd.cache, cacheKey, w); err == nil {
		return
	}

	if v, err, shared := hd.rg.Do(cacheKey, func() (interface{}, error) {
		i, err := hd.handleNodeMetricInGroup(self)

		return i, err
	}); err != nil {
		hd.Log().Err(err).Msg("get node metric")

		HTTP2HandleError(w, err)
	} else {
		HTTP2WriteHalBytes(hd.enc, w, v.([]byte), http.StatusOK)

		if !shared {
			HTTP2WriteCache(w, cacheKey, hd.expireShortLived)
		}
	}
}

type nodeMetricResult struct {
	metric *isaacnetwork.NodeMetrics
	conn   quicstream.ConnInfo
}

func (hd *Handlers) collectNodeMetrics(self bool) ([]nodeMetricResult, error) {
	client := hd.baseClient
	memberList := hd.memberList
	nodeList := hd.staticNodeList

	if client == nil {
		return nil, errors.New("network client is not initialized")
	}

	connInfo := make(map[string]quicstream.ConnInfo)

	if !self {
		memberList.Members(func(node quicmemberlist.Member) bool {
			connInfo[node.ConnInfo().String()] = node.ConnInfo()
			return true
		})
		for _, c := range nodeList {
			connInfo[c.String()] = c
		}
	} else {
		connInfo[hd.node.String()] = hd.node
	}

	results := make([]nodeMetricResult, 0, len(connInfo))

	for key := range connInfo {
		nodeMetric, err := NodeMetric(client, connInfo[key])
		if err != nil {
			continue
		}

		results = append(results, nodeMetricResult{
			metric: nodeMetric,
			conn:   connInfo[key],
		})
	}

	return results, nil
}

func (hd *Handlers) handleNodeMetricInGroup(self bool) (interface{}, error) {
	results, err := hd.collectNodeMetrics(self)
	if err != nil {
		return nil, err
	}

	nodeMetricList := make([]map[string]interface{}, 0, len(results))
	for i := range results {
		nm := map[string]interface{}{
			"node-metric": results[i].metric,
			"conn-info":   results[i].conn,
		}

		nodeMetricList = append(nodeMetricList, nm)
	}

	if i, err := hd.buildNodeMetricHal(nodeMetricList); err != nil {
		return nil, err
	} else {
		return hd.enc.Marshal(i)
	}
}

func (hd *Handlers) handleNodeMetricProm(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	self := ParseBoolQuery(r.URL.Query().Get("self"))

	results, err := hd.collectNodeMetrics(self)
	if err != nil {
		hd.Log().Err(err).Msg("get node metric for prometheus")
		HTTP2HandleError(w, err)

		return
	}

	var b strings.Builder
	writePromNodeMetrics(&b, results)

	w.Header().Set("Content-Type", PrometheusTextMimetype)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
}

func (hd *Handlers) buildNodeMetricHal(ni []map[string]interface{}) (Hal, error) {
	var hal Hal = NewBaseHal(ni, NewHalLink(HandlerPathNodeMetric, nil))

	return hal, nil
}

type promMetricHeader struct {
	help       string
	metricType string
}

var promMetricHeaders = map[string]promMetricHeader{
	"mitum_node_metrics_timestamp_seconds": {
		help:       "Timestamp of node metrics snapshot in Unix seconds.",
		metricType: "gauge",
	},
	"mitum_node_uptime_seconds": {
		help:       "Node uptime in seconds.",
		metricType: "gauge",
	},
	"mitum_node_cumulative_quic_bytes_sent_total": {
		help:       "Total QUIC bytes sent since start.",
		metricType: "counter",
	},
	"mitum_node_cumulative_quic_bytes_received_total": {
		help:       "Total QUIC bytes received since start.",
		metricType: "counter",
	},
	"mitum_node_cumulative_memberlist_broadcasts_total": {
		help:       "Total memberlist broadcasts since start.",
		metricType: "counter",
	},
	"mitum_node_cumulative_memberlist_messages_recv_total": {
		help:       "Total memberlist messages received since start.",
		metricType: "counter",
	},
	"mitum_node_interval_quic_bytes_sent": {
		help:       "QUIC bytes sent within interval.",
		metricType: "gauge",
	},
	"mitum_node_interval_quic_bytes_received": {
		help:       "QUIC bytes received within interval.",
		metricType: "gauge",
	},
	"mitum_node_interval_quic_bytes_per_sec_sent": {
		help:       "Average QUIC bytes per second sent within interval.",
		metricType: "gauge",
	},
	"mitum_node_interval_quic_bytes_per_sec_recv": {
		help:       "Average QUIC bytes per second received within interval.",
		metricType: "gauge",
	},
	"mitum_node_interval_memberlist_broadcasts": {
		help:       "Memberlist broadcasts within interval.",
		metricType: "gauge",
	},
	"mitum_node_interval_memberlist_messages_recv": {
		help:       "Memberlist messages received within interval.",
		metricType: "gauge",
	},
	"mitum_node_interval_memberlist_msgs_per_sec": {
		help:       "Average memberlist messages per second within interval.",
		metricType: "gauge",
	},
	"mitum_node_interval_active_connections": {
		help:       "Active connections observed within interval.",
		metricType: "gauge",
	},
	"mitum_node_interval_active_streams": {
		help:       "Active streams observed within interval.",
		metricType: "gauge",
	},
	"mitum_node_interval_memberlist_members": {
		help:       "Memberlist membership count observed within interval.",
		metricType: "gauge",
	},
	"mitum_node_info_started_timestamp_seconds": {
		help:       "Node start timestamp in Unix seconds.",
		metricType: "gauge",
	},
	"mitum_node_info_suffrage_height": {
		help:       "Current suffrage height reported by the node.",
		metricType: "gauge",
	},
	"mitum_node_info_consensus_members": {
		help:       "Number of consensus members known to the node.",
		metricType: "gauge",
	},
	"mitum_node_info_last_manifest_height": {
		help:       "Height of the last manifest observed by the node.",
		metricType: "gauge",
	},
	"mitum_node_info_last_manifest_proposed_timestamp_seconds": {
		help:       "Proposal timestamp of the last manifest in Unix seconds.",
		metricType: "gauge",
	},
	"mitum_node_info_network_policy_max_operations_in_proposal": {
		help:       "Network policy limit for operations per proposal.",
		metricType: "gauge",
	},
	"mitum_node_info_network_policy_max_suffrage_size": {
		help:       "Maximum suffrage size allowed by network policy.",
		metricType: "gauge",
	},
	"mitum_node_info_network_policy_suffrage_candidate_lifespan": {
		help:       "Suffrage candidate lifespan configured in network policy.",
		metricType: "gauge",
	},
	"mitum_node_info_network_policy_suffrage_expel_lifespan": {
		help:       "Suffrage expel lifespan configured in network policy.",
		metricType: "gauge",
	},
	"mitum_node_info_network_policy_empty_proposal_no_block": {
		help:       "Whether empty proposals skip block creation (1=yes, 0=no).",
		metricType: "gauge",
	},
	"mitum_node_info_last_vote_height": {
		help:       "Block height from the node's last vote.",
		metricType: "gauge",
	},
	"mitum_node_info_last_vote_round": {
		help:       "Round from the node's last vote.",
		metricType: "gauge",
	},
	"mitum_node_info_last_vote_state": {
		help:       "Node last vote state, labelled by stage and result.",
		metricType: "gauge",
	},
	"mitum_resource_memory_alloc_bytes": {
		help:       "Currently allocated heap memory in bytes.",
		metricType: "gauge",
	},
	"mitum_resource_memory_total_alloc_bytes": {
		help:       "Total heap bytes allocated since start.",
		metricType: "counter",
	},
	"mitum_resource_memory_sys_bytes": {
		help:       "Overall bytes obtained from the OS.",
		metricType: "gauge",
	},
	"mitum_resource_memory_heap_alloc_bytes": {
		help:       "Bytes allocated on the heap and still in use.",
		metricType: "gauge",
	},
	"mitum_resource_memory_heap_sys_bytes": {
		help:       "Bytes obtained from the OS for heap use.",
		metricType: "gauge",
	},
	"mitum_resource_memory_heap_idle_bytes": {
		help:       "Heap bytes not in use.",
		metricType: "gauge",
	},
	"mitum_resource_memory_heap_inuse_bytes": {
		help:       "Heap bytes in use.",
		metricType: "gauge",
	},
	"mitum_resource_memory_heap_released_bytes": {
		help:       "Heap bytes released back to the OS.",
		metricType: "gauge",
	},
	"mitum_resource_memory_stack_inuse_bytes": {
		help:       "Stack bytes currently in use by goroutines.",
		metricType: "gauge",
	},
	"mitum_resource_memory_stack_sys_bytes": {
		help:       "Stack bytes obtained from the OS.",
		metricType: "gauge",
	},
	"mitum_resource_memory_next_gc_bytes": {
		help:       "Target heap size of the next GC cycle.",
		metricType: "gauge",
	},
	"mitum_resource_memory_heap_objects": {
		help:       "Number of allocated heap objects.",
		metricType: "gauge",
	},
	"mitum_resource_memory_mspan_inuse_bytes": {
		help:       "Bytes of in-use mspan structures.",
		metricType: "gauge",
	},
	"mitum_resource_memory_mspan_sys_bytes": {
		help:       "Bytes of memory obtained for mspan structures.",
		metricType: "gauge",
	},
	"mitum_resource_memory_mcache_inuse_bytes": {
		help:       "Bytes of in-use mcache structures.",
		metricType: "gauge",
	},
	"mitum_resource_memory_mcache_sys_bytes": {
		help:       "Bytes obtained for mcache structures.",
		metricType: "gauge",
	},
	"mitum_resource_memory_buck_hash_sys_bytes": {
		help:       "Profiler bucket hash table bytes.",
		metricType: "gauge",
	},
	"mitum_resource_memory_gc_sys_bytes": {
		help:       "GC metadata bytes.",
		metricType: "gauge",
	},
	"mitum_resource_memory_other_sys_bytes": {
		help:       "Other runtime system bytes.",
		metricType: "gauge",
	},
	"mitum_resource_memory_last_gc_timestamp_seconds": {
		help:       "Timestamp of the last completed GC in Unix seconds.",
		metricType: "gauge",
	},
	"mitum_resource_memory_pause_total_seconds": {
		help:       "Total GC pause time in seconds.",
		metricType: "counter",
	},
	"mitum_resource_memory_num_gc_total": {
		help:       "Total number of completed GC cycles.",
		metricType: "counter",
	},
	"mitum_resource_memory_num_forced_gc_total": {
		help:       "Total number of forced GC cycles.",
		metricType: "counter",
	},
	"mitum_resource_memory_gc_cpu_fraction": {
		help:       "Fraction of CPU time spent in GC.",
		metricType: "gauge",
	},
	"mitum_resource_memory_enable_gc": {
		help:       "Whether GC is currently enabled (1) or disabled (0).",
		metricType: "gauge",
	},
	"mitum_resource_memory_debug_gc": {
		help:       "Whether GC debug mode is enabled (1) or disabled (0).",
		metricType: "gauge",
	},
}

func writePromNodeMetrics(b *strings.Builder, results []nodeMetricResult) {
	headersWritten := map[string]bool{}

	if len(results) == 0 {
		b.WriteString("# No node metrics available\n")

		return
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].conn.String() < results[j].conn.String()
	})

	for i := range results {
		if results[i].metric == nil {
			continue
		}

		nodeLabel := results[i].conn.String()
		m := results[i].metric

		writePromSample(
			b,
			"mitum_node_metrics_timestamp_seconds",
			map[string]string{"node": nodeLabel},
			strconv.FormatFloat(float64(m.Timestamp.UnixNano())/1e9, 'f', -1, 64),
			headersWritten,
		)
		writePromSample(
			b,
			"mitum_node_uptime_seconds",
			map[string]string{"node": nodeLabel},
			strconv.FormatFloat(m.Uptime.Seconds(), 'f', -1, 64),
			headersWritten,
		)

		writePromSample(
			b,
			"mitum_node_cumulative_quic_bytes_sent_total",
			map[string]string{"node": nodeLabel},
			strconv.FormatUint(m.Cumulative.QuicBytesSent, 10),
			headersWritten,
		)
		writePromSample(
			b,
			"mitum_node_cumulative_quic_bytes_received_total",
			map[string]string{"node": nodeLabel},
			strconv.FormatUint(m.Cumulative.QuicBytesReceived, 10),
			headersWritten,
		)
		writePromSample(
			b,
			"mitum_node_cumulative_memberlist_broadcasts_total",
			map[string]string{"node": nodeLabel},
			strconv.FormatUint(m.Cumulative.MemberlistBroadcasts, 10),
			headersWritten,
		)
		writePromSample(
			b,
			"mitum_node_cumulative_memberlist_messages_recv_total",
			map[string]string{"node": nodeLabel},
			strconv.FormatUint(m.Cumulative.MemberlistMessagesRecv, 10),
			headersWritten,
		)

		intervalKeys := make([]string, 0, len(m.Intervals))
		for k := range m.Intervals {
			intervalKeys = append(intervalKeys, k)
		}
		sort.Strings(intervalKeys)

		for _, key := range intervalKeys {
			im := m.Intervals[key]
			labelSet := map[string]string{
				"node":     nodeLabel,
				"interval": key,
			}

			writePromSample(
				b,
				"mitum_node_interval_quic_bytes_sent",
				labelSet,
				strconv.FormatUint(im.QuicBytesSent, 10),
				headersWritten,
			)
			writePromSample(
				b,
				"mitum_node_interval_quic_bytes_received",
				labelSet,
				strconv.FormatUint(im.QuicBytesReceived, 10),
				headersWritten,
			)
			writePromSample(
				b,
				"mitum_node_interval_quic_bytes_per_sec_sent",
				labelSet,
				strconv.FormatFloat(im.QuicBytesPerSecSent, 'f', -1, 64),
				headersWritten,
			)
			writePromSample(
				b,
				"mitum_node_interval_quic_bytes_per_sec_recv",
				labelSet,
				strconv.FormatFloat(im.QuicBytesPerSecRecv, 'f', -1, 64),
				headersWritten,
			)
			writePromSample(
				b,
				"mitum_node_interval_memberlist_broadcasts",
				labelSet,
				strconv.FormatUint(im.MemberlistBroadcasts, 10),
				headersWritten,
			)
			writePromSample(
				b,
				"mitum_node_interval_memberlist_messages_recv",
				labelSet,
				strconv.FormatUint(im.MemberlistMessagesRecv, 10),
				headersWritten,
			)
			writePromSample(
				b,
				"mitum_node_interval_memberlist_msgs_per_sec",
				labelSet,
				strconv.FormatFloat(im.MemberlistMsgsPerSec, 'f', -1, 64),
				headersWritten,
			)
			writePromSample(
				b,
				"mitum_node_interval_active_connections",
				labelSet,
				strconv.FormatInt(im.ActiveConnections, 10),
				headersWritten,
			)
			writePromSample(
				b,
				"mitum_node_interval_active_streams",
				labelSet,
				strconv.FormatInt(im.ActiveStreams, 10),
				headersWritten,
			)
			writePromSample(
				b,
				"mitum_node_interval_memberlist_members",
				labelSet,
				strconv.FormatInt(im.MemberlistMembers, 10),
				headersWritten,
			)
		}
	}
}

func writePromSample(
	b *strings.Builder,
	name string,
	labels map[string]string,
	value string,
	headersWritten map[string]bool,
) {
	if value == "" {
		return
	}

	meta, ok := promMetricHeaders[name]
	if ok && !headersWritten[name] {
		b.WriteString("# HELP ")
		b.WriteString(name)
		b.WriteString(" ")
		b.WriteString(meta.help)
		b.WriteByte('\n')
		b.WriteString("# TYPE ")
		b.WriteString(name)
		b.WriteString(" ")
		b.WriteString(meta.metricType)
		b.WriteByte('\n')

		headersWritten[name] = true
	}

	b.WriteString(name)

	if len(labels) > 0 {
		labelKeys := make([]string, 0, len(labels))
		for k := range labels {
			labelKeys = append(labelKeys, k)
		}
		sort.Strings(labelKeys)

		b.WriteByte('{')
		for i := range labelKeys {
			if i > 0 {
				b.WriteByte(',')
			}

			b.WriteString(labelKeys[i])
			b.WriteByte('=')
			b.WriteByte('"')
			b.WriteString(sanitizePromLabelValue(labels[labelKeys[i]]))
			b.WriteByte('"')
		}
		b.WriteByte('}')
	}

	b.WriteByte(' ')
	b.WriteString(value)
	b.WriteByte('\n')
}

func sanitizePromLabelValue(v string) string {
	v = strings.ReplaceAll(v, "\\", "\\\\")
	v = strings.ReplaceAll(v, "\n", " ")
	v = strings.ReplaceAll(v, "\r", " ")
	v = strings.ReplaceAll(v, "\t", " ")
	v = strings.ReplaceAll(v, `"`, `\"`)

	return v
}

func NodeMetric(client *isaacnetwork.BaseClient, connInfo quicstream.ConnInfo) (*isaacnetwork.NodeMetrics, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	stream, _, err := client.Dial(ctx, connInfo)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = client.Close()
	}()

	header := isaacnetwork.NewNodeMetricsRequestHeader("1m")

	var nodeMetric *isaacnetwork.NodeMetrics
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

			ni, ok := h.(isaacnetwork.NodeMetrics)
			if !ok {
				return errors.Errorf("expected isaacnetwork.NodeMetrics, not %T", v)
			}

			nodeMetric = &ni

			return nil
		}
	})
	if err != nil {
		return nil, err
	}

	return nodeMetric, nil
}
