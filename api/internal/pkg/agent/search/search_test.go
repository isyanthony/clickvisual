package search

import (
	"fmt"
	"testing"
)

func TestGoroutineCalc(t *testing.T) {
	// 1696070784
	// 60 * 60 * 24 * 365 * 2
	// st, et := int64(1634184933), int64(1662832024) // 358
	st, et := int64(1698716774), int64(1698718344) // 1000
	req := Request{
		// Debug:          true,
		IsChartRequest: true,
		StartTime:      st,
		EndTime:        et,
		Dir:            "./test",
		// IsK8S:        true,
		// Limit: 10,
		// Dir:          "logs",
		// K8SContainer: []string{"redis"},
		Interval: ChartsIntervalConvert(et - st),
	}
	fmt.Println(req)

	fmt.Printf("st: %d, et: %d, interval: %d, times: %d\n", req.StartTime, req.EndTime, req.Interval, (req.EndTime-req.StartTime)/req.Interval)
}
