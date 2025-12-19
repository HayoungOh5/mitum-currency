package utils

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/armon/go-metrics"
	"github.com/pkg/errors"
)

func HasValue(slice interface{}, value interface{}) (bool, error) {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return false, errors.Errorf("Invalid data-type, Not Slice")
	}

	for i := 0; i < v.Len(); i++ {
		if v.Index(i).Interface() == value {
			return true, nil
		}
	}

	return false, nil
}

func MustHasValue(slice interface{}, value interface{}) (interface{}, error) {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return nil, errors.Errorf("Invalid data-type, Not Slice")
	}

	var nSlice reflect.Value
	for i := 0; i < v.Len(); i++ {
		if v.Index(i).Interface() == value {
			nSlice = reflect.MakeSlice(v.Type(), v.Len(), v.Cap())
			reflect.Copy(nSlice, v)
			break
		}
		if i == v.Len()-1 {
			nSlice = reflect.MakeSlice(v.Type(), v.Len(), v.Cap())
			reflect.Copy(nSlice, v)
			nSlice = reflect.Append(nSlice, reflect.ValueOf(value))
		}
	}

	return nSlice.Interface(), nil
}

func HasFieldValue(slice interface{}, fieldName string, value interface{}) (bool, error) {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return false, errors.Errorf("Invalid data-type, Not Slice")
	}

	if len(fieldName) < 1 {
		return false, errors.Errorf("empty field name")
	}

	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		if item.Kind() == reflect.Struct {
			field := item.FieldByName(fieldName)
			if !field.IsValid() {
				panic("No such field: " + fieldName)
			}
			if field.Interface() == value {
				return true, nil
			}
		}
	}

	return false, nil
}

func HasFieldAndSliceValue(
	slice interface{},
	fieldName, sliceFieldName string,
	fieldValue, sliceFieldValue interface{},
) (bool, bool, error) {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return false, false, errors.Errorf("Invalid data-type, Not Slice")
	}

	if len(fieldName) < 1 {
		return false, false, errors.Errorf("empty field name")
	} else if len(sliceFieldName) < 1 {
		return false, false, errors.Errorf("empty field name for slice")
	}

	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		if item.Kind() != reflect.Struct {
			return false, false, errors.Errorf("Invalid data-type, Not Struct")
		}
		field := item.FieldByName(fieldName)
		if !field.IsValid() {
			return false, false, errors.Errorf("No such field: " + fieldName)
		}
		if field.Interface() != fieldValue {
			return false, false, nil
		}
		sliceField := item.FieldByName(sliceFieldName)
		if !sliceField.IsValid() {
			return true, false, errors.Errorf("No such field: " + sliceFieldName)
		}
		if sliceField.Kind() != reflect.Slice {
			return true, false, errors.Errorf("Invalid data-type, Not Slice")
		}
		for j := 0; j < sliceField.Len(); j++ {
			if sliceField.Index(j).Interface() == sliceFieldValue {
				return true, true, nil
			}
			if i == sliceField.Len()-1 {
				return true, false, nil
			}
		}
	}
	return false, false, nil
}

func InitMetrics() error {
	// 1. InmemSink 생성
	inm := metrics.NewInmemSink(500*time.Millisecond, 2*time.Second)

	// 2. 글로벌 설정 ("mitum" 접두어 사용)
	metrics.NewGlobal(metrics.DefaultConfig("mitum"), inm)

	// 3. 모니터링 고루틴
	go func() {
		// 보기 편하게 3초마다 출력
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			data := inm.Data()
			if len(data) == 0 {
				continue
			}
			latest := data[0]

			fmt.Println("\n========== [ Mitum Metric Report ] ==========")

			// ------------------------------------------------------
			// [Helper] 호스트명(MacBook...)이 섞여있는 Gauge 찾기 함수
			// ------------------------------------------------------
			findGauge := func(keyword string) (float32, bool) {
				for k, v := range latest.Gauges {
					// 키에 해당 키워드가 포함되어 있으면 값 리턴
					if strings.Contains(k, keyword) {
						return v.Value, true
					}
				}
				return 0, false
			}

			// 1. Node Health (Gauge: 호스트명이 섞여있으므로 검색 방식 사용)
			// 로그에 'health.score'가 안 보여서 대신 'node.instances;node_state=alive'를 체크합니다.
			if val, ok := findGauge("node.instances;node_state=alive"); ok {
				fmt.Printf(">> Alive Nodes   : %v (Active members)\n", val)
			}
			// GC 횟수 (Runtime 메트릭)
			if val, ok := findGauge("runtime.total_gc_runs"); ok {
				fmt.Printf(">> Total GC Runs : %v\n", val)
			}

			// 2. Network Traffic (Counter: 'mitum.' 접두어 필요)
			fmt.Println(">> Network Traffic")
			if c, ok := latest.Counters["mitum.memberlist.udp.sent"]; ok {
				fmt.Printf(" - UDP Sent      : %d\n", c.Count)
			}
			if c, ok := latest.Counters["mitum.memberlist.tcp.sent"]; ok {
				fmt.Printf(" - TCP Sent      : %d\n", c.Count)
			}
			if c, ok := latest.Counters["mitum.memberlist.msg.alive"]; ok {
				fmt.Printf(" - Alive Msgs    : %d\n", c.Count)
			}

			// 3. Performance / Timers (Sample: 'mitum.' 접두어 필요)
			// 로그에 있는 키들을 정확히 매핑했습니다.
			fmt.Println(">> Performance (Avg / Max ms)")

			// Gossip 속도
			if s, ok := latest.Samples["mitum.memberlist.gossip"]; ok {
				fmt.Printf(" - Gossip        : %.2f / %.2f\n", s.Mean, s.Max)
			}
			if s, ok := latest.Samples["mitum.memberlist.fastgossip"]; ok {
				fmt.Printf(" - Fast Gossip   : %.2f / %.2f\n", s.Mean, s.Max)
			}

			// 노드 탐색 속도 (이게 느리면 네트워크 불안정)
			if s, ok := latest.Samples["mitum.memberlist.probeNode"]; ok {
				fmt.Printf(" - Probe Node    : %.2f / %.2f\n", s.Mean, s.Max)
			}

			// 데이터 동기화 속도
			if s, ok := latest.Samples["mitum.memberlist.pushPullNode"]; ok {
				fmt.Printf(" - Push/Pull     : %.2f / %.2f\n", s.Mean, s.Max)
			}

			fmt.Println("=============================================")
		}
	}()

	return nil
}
