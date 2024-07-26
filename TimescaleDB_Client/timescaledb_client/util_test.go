package timescaledb_client

import (
	"reflect"
	"testing"
)

func TestGetQueryTemplate(t *testing.T) {
	tests := []struct {
		name              string
		queryString       string
		expectedTemplate  string
		expectedStartTime int64
		expectedEndTime   int64
		expectedTags      []string
	}{
		{
			name:              "cpu",
			queryString:       `SELECT time_bucket('15 minute', time) as bucket,hostname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE hostname IN ('host_1','host_45','host_23') AND time >= '2022-10-30 06:00:00 +0000' AND time < '2022-11-20 06:00:00.999999 +0000' GROUP BY hostname,bucket ORDER BY hostname,bucket`,
			expectedTemplate:  `SELECT time_bucket('15 minute', time) as bucket,hostname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE ? AND time >= '?' AND time < '?' GROUP BY hostname,bucket ORDER BY hostname,bucket`,
			expectedStartTime: 1667109600,
			expectedEndTime:   1668924000,
			expectedTags:      []string{"hostname=host_1", "hostname=host_23", "hostname=host_45"},
		},
		{
			name:              "iot",
			queryString:       `SELECT time_bucket('15 minute', time) as bucket,name,avg(latitude),avg(longitude) FROM readings WHERE name IN ('truck_1','truck_45','truck_23') AND time >= '2022-01-01 08:02:00 +0000' AND time < '2022-01-01 08:09:00 +0000' GROUP BY name,bucket ORDER BY name,bucket`,
			expectedTemplate:  `SELECT time_bucket('15 minute', time) as bucket,name,avg(latitude),avg(longitude) FROM readings WHERE ? AND time >= '?' AND time < '?' GROUP BY name,bucket ORDER BY name,bucket`,
			expectedStartTime: 1641024120,
			expectedEndTime:   1641024540,
			expectedTags:      []string{"name=truck_1", "name=truck_23", "name=truck_45"},
		},
		{
			name:              "cpu predicate",
			queryString:       `SELECT time as bucket,hostname,usage_user,usage_guest,usage_nice FROM cpu WHERE hostname IN ('host_10','host_45','host_23') AND time >= '2022-01-01 08:00:00 +0000' AND time < '2022-01-01 09:00:00 +0000' AND usage_user > 10 AND usage_guest > 10 ORDER BY hostname,bucket`,
			expectedTemplate:  `SELECT time as bucket,hostname,usage_user,usage_guest,usage_nice FROM cpu WHERE ? AND time >= '?' AND time < '?' AND usage_user > 10 AND usage_guest > 10 ORDER BY hostname,bucket`,
			expectedStartTime: 1641024000,
			expectedEndTime:   1641027600,
			expectedTags:      []string{"hostname=host_10", "hostname=host_23", "hostname=host_45"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replaced, startTime, endTime, tags := GetQueryTemplate(tt.queryString)

			//fmt.Println(replaced)

			if replaced != tt.expectedTemplate {
				t.Errorf("replaces:%s", replaced)
				t.Errorf("expected:%s", tt.expectedTemplate)
			}
			if startTime != tt.expectedStartTime {
				t.Errorf("replaces:%d", startTime)
				t.Errorf("expected:%d", tt.expectedStartTime)
			}
			if endTime != tt.expectedEndTime {
				t.Errorf("replaces:%d", endTime)
				t.Errorf("expected:%d", tt.expectedEndTime)
			}
			for i, tag := range tags {
				if tag != tt.expectedTags[i] {
					t.Errorf("replaces:%s", tag)
					t.Errorf("expected:%s", tt.expectedTags[i])
				}
			}
		})
	}
}

func TestDataTypeArrayFromSF(t *testing.T) {
	sfStringArr := []string{
		"ts[int64],tbname[string],index[int64]",
		"ts[int64],tbname[string],index[int64],location[string],randtag[string]",
		"Int[int64],tbname[string],Float[float64],Bool[bool],String[string]",
	}
	expected := [][]string{
		{"int64", "string", "int64"},
		{"int64", "string", "int64", "string", "string"},
		{"int64", "string", "float64", "bool", "string"},
	}

	for i := range sfStringArr {
		datatypes := GetDataTypeArrayFromSF(sfStringArr[i])
		if !reflect.DeepEqual(datatypes, expected[i]) {
			t.Errorf("datatypes:%s", datatypes)
			t.Errorf("expected:%s", expected[i])
		}
	}

}

func TestTimeInt64ToString(t *testing.T) {
	tests := []struct {
		name       string
		timeInt    []int64
		timeString []string
	}{
		{
			name:       "1",
			timeInt:    []int64{1640995320, 1640995740, 1672444800, 1671235200},
			timeString: []string{"2022-01-01 08:02:00 +0800", "2022-01-01 08:09:00 +0800", "2022-12-31 08:00:00 +0800", "2022-12-17 08:00:00 +0800"},
		},
		{
			name:       "1",
			timeInt:    []int64{1644969600, 1645012800},
			timeString: []string{"2022-02-16 08:00:00 +0800", "2022-02-16 20:00:00 +0800"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := range tt.timeInt {
				str := TimeInt64ToString(tt.timeInt[i])
				if str != tt.timeString[i] {
					t.Errorf("get : %s\nwant : %s\n", str, tt.timeString[i])
				}
			}
		})
	}
}

func TestTimeStringToInt64(t *testing.T) {
	tests := []struct {
		name       string
		timeString []string
		timeInt    []int64
	}{
		{
			name:       "1",
			timeString: []string{"2022-01-01 08:02:00 +0000", "2022-01-01 08:09:00 +0800", "2022-12-31 08:00:00.999 +0800", "2022-12-17 08:00:00.999 +0800", "2022-12-27 21:00:00.999999 +0000"},
			timeInt:    []int64{1641024120, 1640995740, 1672444800, 1671235200, 1672174800},
		},
		{
			name:       "2",
			timeString: []string{"2022-01-01 08:00:00 +0000", "2022-01-01 09:00:00 +0000"},
			timeInt:    []int64{1641024000, 1641027600},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := range tt.timeString {
				number := TimeStringToInt64(tt.timeString[i])
				if number != tt.timeInt[i] {
					t.Errorf("get : %d\nwant : %d\n", number, tt.timeInt[i])
				}
			}
		})
	}
}
