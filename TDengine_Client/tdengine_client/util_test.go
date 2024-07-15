package tdengine_client

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
			queryString:       `SELECT _wstart AS ts,tbname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE tbname IN ('host_1','host_45','host_23') AND ts >= 1640995320000 AND ts < 1640995800000 PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`,
			expectedTemplate:  `SELECT _wstart AS ts,tbname,avg(usage_user),avg(usage_guest),avg(usage_nice) FROM cpu WHERE tbname IN (?) AND ts >= ? AND ts < ? PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`,
			expectedStartTime: 1640995320000,
			expectedEndTime:   1640995800000,
			expectedTags:      []string{"hostname=host_1", "hostname=host_23", "hostname=host_45"},
		},
		{
			name:              "iot",
			queryString:       `SELECT _wstart as ts,tbname,avg(latitude),avg(longitude) FROM readings WHERE tbname IN ('r_truck_1','r_truck_45','r_truck_23') AND ts >= 1640995320000 AND ts < 1640995800000 PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`,
			expectedTemplate:  `SELECT _wstart as ts,tbname,avg(latitude),avg(longitude) FROM readings WHERE tbname IN (?) AND ts >= ? AND ts < ? PARTITION BY tbname INTERVAL(1m) ORDER BY tbname,ts`,
			expectedStartTime: 1640995320000,
			expectedEndTime:   1640995800000,
			expectedTags:      []string{"name=r_truck_1", "name=r_truck_23", "name=r_truck_45"},
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

func TestColumnDataType(t *testing.T) {
	tests := []struct {
		name     string
		colType  []uint8
		datatype []string
	}{
		{
			name:     "1",
			colType:  []uint8{9, 8, 7, 6, 5, 4, 1},
			datatype: []string{"int64", "string", "float64", "float64", "int64", "int64", "bool"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			types := DataTypeFromColumn(tt.colType)
			for i := range types {
				if types[i] != tt.datatype[i] {
					t.Errorf("get : %s\nwant : %s\n", types[i], tt.datatype[i])
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
			timeInt:    []int64{1640995320000, 1640995740000},
			timeString: []string{"2022-01-01 08:02:00.000", "2022-01-01 08:09:00.000"},
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
			timeString: []string{"2022-01-01 08:02:00.000", "2022-01-01 08:09:00.000"},
			timeInt:    []int64{1640995320000, 1640995740000},
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
