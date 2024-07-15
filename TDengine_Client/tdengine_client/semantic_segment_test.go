package tdengine_client

import "testing"

func TestSplitPartialSegment(t *testing.T) {
	tests := []struct {
		name           string
		segment        string
		partialSegment string
		fields         string
		metric         string
		tags           []string
		sepSegments    []string
	}{
		{
			name:           "1",
			segment:        "{(cpu.hostname=host_0)(cpu.hostname=host_1)}#{usage_nice[int64],usage_steal[int64],usage_guest[int64]}#{empty}#{mean,5m}",
			partialSegment: "#{usage_nice[int64],usage_steal[int64],usage_guest[int64]}#{empty}#{mean,5m}",
			fields:         "usage_nice[int64],usage_steal[int64],usage_guest[int64]",
			metric:         "cpu",
			tags:           []string{"hostname=host_0", "hostname=host_1"},
			sepSegments: []string{
				"{(cpu.hostname=host_0)}#{usage_nice[int64],usage_steal[int64],usage_guest[int64]}#{empty}#{mean,5m}",
				"{(cpu.hostname=host_1)}#{usage_nice[int64],usage_steal[int64],usage_guest[int64]}#{empty}#{mean,5m}",
			},
		},
		{
			name:           "2",
			segment:        "{(readings.name=truck_0)(readings.name=truck_12)}#{velocity[float64],fuel_consumption[float64],grade[float64]}#{(velocity>50[int64])}#{mean,5m}",
			partialSegment: "#{velocity[float64],fuel_consumption[float64],grade[float64]}#{(velocity>50[int64])}#{mean,5m}",
			fields:         "velocity[float64],fuel_consumption[float64],grade[float64]",
			metric:         "readings",
			tags:           []string{"name=truck_0", "name=truck_12"},
			sepSegments: []string{
				"{(readings.name=truck_0)}#{velocity[float64],fuel_consumption[float64],grade[float64]}#{(velocity>50[int64])}#{mean,5m}",
				"{(readings.name=truck_12)}#{velocity[float64],fuel_consumption[float64],grade[float64]}#{(velocity>50[int64])}#{mean,5m}",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps, fields, metric := SplitPartialSegment(tt.segment)
			sepSegments := GetSingleSegment(metric, ps, tt.tags)
			if ps != tt.partialSegment {
				t.Errorf("get : %s\nwant : %s\n", ps, tt.partialSegment)
			}
			if fields != tt.fields {
				t.Errorf("get : %s\nwant : %s\n", fields, tt.fields)
			}
			if metric != tt.metric {
				t.Errorf("get : %s\nwant : %s\n", metric, tt.metric)
			}
			for i, ss := range sepSegments {
				if ss != tt.sepSegments[i] {
					t.Errorf("get : %s\nwant : %s\n", ss, tt.sepSegments[i])
				}
			}
		})
	}
}
