package tdengine_client

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/tsbs/pkg/targets/tdengine/async"
	"log"
	"reflect"
	"strconv"
	"strings"
)

// ByteArrayToResponseWithDatatype 字节数组转换成结果类型
func ByteArrayToResponseWithDatatype(byteArray []byte, datatypes []string) ([]*async.ExecResult, int, []uint8, [][]int64, [][]string) {

	/* 没有数据 */
	if len(byteArray) == 0 {
		return nil, 0, nil, nil, nil
	}

	resp := make([]*async.ExecResult, 0)

	valuess := make([][][]driver.Value, 0) // 存放不同表(Series)的所有 values
	values := make([][]driver.Value, 0)    // 存放一张表的 values
	value := make([]driver.Value, 0)       // 存放 values 里的一行数据

	seprateSemanticSegments := make([]string, 0) // 存放所有表各自的SCHEMA
	seriesLength := make([]int64, 0)             // 每张表的数据的总字节数

	flagNum := 0
	flagArr := make([]uint8, 0)
	timeRangeArr := make([][]int64, 0) // 每张表的剩余待查询时间范围
	tagArr := make([][]string, 0)

	var curSeg string        // 当前表的语义段
	var curLen int64         // 当前表的数据的总字节数
	index := 0               // byteArray 数组的索引，指示当前要转换的字节的位置
	length := len(byteArray) // Get()获取的总字节数

	per_index := 0
	is_start := 0
	/* 转换 */
	for index < length {
		/* 结束转换 */
		if index == length-2 { // 索引指向数组的最后两字节
			if byteArray[index] == 13 && byteArray[index+1] == 10 { // "\r\n"，表示Get()返回的字节数组的末尾，结束转换		Get()除了返回查询数据之外，还会在数据末尾添加一个 "\r\n",如果读到这个组合，说明到达数组末尾
				break
			} else {
				log.Fatal(errors.New("expect CRLF in the end of []byte"))
			}
		}
		if is_start == 1 && per_index == index {
			return nil, 0, nil, nil, nil
		}
		is_start = 1
		per_index = index

		curSeg = ""
		curLen = 0
		if byteArray[index] == 123 && byteArray[index+1] == 40 { // "{(" ASCII码	表示语义段的开始位置
			ssStartIdx := index
			for byteArray[index] != 32 { // ' '空格，表示语义段的结束位置的后一位
				index++
			}
			ssEndIdx := index                               // 此时索引指向 len 前面的 空格
			curSeg = string(byteArray[ssStartIdx:ssEndIdx]) // 读取所有表示语义段的字节，直接转换为字符串
			seprateSemanticSegments = append(seprateSemanticSegments, curSeg)

			// todo 时间范围
			index++ // uint8
			flag := uint8(byteArray[index])
			index++
			flagArr = append(flagArr, flag)
			if flag == 1 {
				flagNum++
				singleTimeRange := make([]int64, 2)
				ftimeStartIdx := index // 索引指向第一个时间戳
				index += 8
				ftimeEndIdx := index // 索引指向 len 后面一位的回车符 '\r' ，再后面一位是 '\n'
				tmpBytes := byteArray[ftimeStartIdx:ftimeEndIdx]
				startTime, err := ByteArrayToInt64(tmpBytes) // 读取 len ，转换为int64
				if err != nil {
					log.Fatal(err)
				}
				startTime *= 1000
				singleTimeRange[0] = startTime

				stimeStartIdx := index // 索引指向第一个时间戳
				index += 8
				stimeEndIdx := index // 索引指向 len 后面一位的回车符 '\r' ，再后面一位是 '\n'
				tmpBytes = byteArray[stimeStartIdx:stimeEndIdx]
				endTime, err := ByteArrayToInt64(tmpBytes) // 读取 len ，转换为int64
				if err != nil {
					log.Fatal(err)
				}
				endTime *= 1000
				singleTimeRange[1] = endTime

				//fmt.Printf("%d %d\n", startTime, endTime)

				timeRangeArr = append(timeRangeArr, singleTimeRange)
			} else {
				singleTimeRange := make([]int64, 2)
				singleTimeRange[0] = 0
				singleTimeRange[1] = 0
				timeRangeArr = append(timeRangeArr, singleTimeRange)
			}

			// length
			//index++              // 空格后面的8字节是表示一张表中数据总字节数的int64
			lenStartIdx := index // 索引指向 len 的第一个字节
			index += 8
			lenEndIdx := index // 索引指向 len 后面一位的回车符 '\r' ，再后面一位是 '\n'
			tmpBytes := byteArray[lenStartIdx:lenEndIdx]
			serLen, err := ByteArrayToInt64(tmpBytes) // 读取 len ，转换为int64
			if err != nil {
				log.Fatal(err)
			}
			curLen = serLen
			seriesLength = append(seriesLength, curLen)
		}

		/* 根据数据类型转换每行数据*/
		bytesPerLine := BytesPerLine(datatypes) // 每行字节数
		lines := int(curLen) / bytesPerLine     // 数据行数
		sttIdx := strings.Index(curSeg, "=")
		endIdx := strings.Index(curSeg, ")")
		//if sttIdx < 0 || endIdx < 0 {
		//	fmt.Println("byteArray: ", string(byteArray[17000:17200]))
		//	fmt.Println("curSeg: ", curSeg)
		//	fmt.Println("curLen: ", curLen)
		//	fmt.Println("byte array len: ", len(byteArray))
		//	fmt.Println("seprateSemanticSegments length: ", len(seprateSemanticSegments))
		//	fmt.Println("seprateSemanticSegment: ", seprateSemanticSegments)
		//}
		tag := curSeg[sttIdx+1 : endIdx]
		avg := strings.Contains(strings.ToLower(curSeg), "mean")
		values = nil
		for len(values) < lines { // 按行读取一张表中的所有数据
			value = nil
			for i, d := range datatypes { // 每次处理一行, 遍历一行中的所有列
				if i == 1 {
					value = append(value, tag)
					continue
				}
				if avg && i > 1 && datatypes[i] == "int64" {
					d = "float64"
				}
				switch d { // 根据每列的数据类型选择转换方法
				case "string":
					bStartIdx := index
					index += STRINGBYTELENGTH //	索引指向当前数据的后一个字节
					bEndIdx := index
					tmp := ByteArrayToString(byteArray[bStartIdx:bEndIdx])
					value = append(value, tmp)
					break
				case "bool":
					bStartIdx := index
					index += 1 //	索引指向当前数据的后一个字节
					bEndIdx := index
					tmp, err := ByteArrayToBool(byteArray[bStartIdx:bEndIdx])
					if err != nil {
						log.Fatal(err)
					}
					value = append(value, tmp)
					break
				case "int64":
					iStartIdx := index
					index += 8 // 索引指向当前数据的后一个字节
					iEndIdx := index
					tmp, err := ByteArrayToInt64(byteArray[iStartIdx:iEndIdx])
					if err != nil {
						log.Fatal(err)
					}
					if i == 0 {
						tmp *= 1000
					}
					value = append(value, tmp)
					break
				case "float64":
					fStartIdx := index
					index += 8 // 索引指向当前数据的后一个字节
					fEndIdx := index
					tmp, err := ByteArrayToFloat64(byteArray[fStartIdx:fEndIdx])
					if err != nil {
						log.Fatal(err)
					}
					value = append(value, tmp)
					break
				default: // float64
					sStartIdx := index
					index += 8 // 索引指向当前数据的后一个字节
					sEndIdx := index
					tmp, err := ByteArrayToFloat64(byteArray[sStartIdx:sEndIdx])
					if err != nil {
						log.Fatal(err)
					}
					value = append(value, tmp) // 存放一行数据中的每一列
					break
				}
			}
			values = append(values, value) // 存放一张表的每一行数据

		}
		valuess = append(valuess, values)
	}

	for i, s := range seprateSemanticSegments {
		messages := strings.Split(s, "#")
		ssm := messages[0][2 : len(messages[0])-2] // 去掉SM两侧的 大括号和小括号
		merged := strings.Split(ssm, ",")
		nameIndex := strings.Index(merged[0], ".") // 提取 measurement name
		/* 取出所有tag 当前只处理了一个tag的情况*/
		tag := merged[0][nameIndex+1 : len(merged[0])]
		eqIdx := strings.Index(tag, "=") // tag 和 value 由  "=" 连接
		if eqIdx <= 0 {                  // 没有等号说明没有tag
			if tag == "*" {

			} else {
				break
			}

		} else {
			key := tag[:eqIdx] // Response 中的 tag 结构为 map[string]string
			val := tag[eqIdx+1 : len(tag)]

			tmpTagArr := make([]string, 2)
			tmpTagArr[0] = key
			tmpTagArr[1] = val
			tagArr = append(tagArr, tmpTagArr)
		}
		/* 处理sf 如果有聚合函数，列名要用函数名，否则用sf中的列名*/
		columns := make([]string, 0)
		sf := "ts[int64]," // sf中去掉了第一列的time，还原时要添上
		sf += messages[1][1 : len(messages[1])-1]
		sg := messages[3][1 : len(messages[3])-1]
		splitSg := strings.Split(sg, ",")
		aggr := splitSg[0]
		if aggr == "mean" {
			aggr = "avg"
		} // 聚合函数名，小写的
		if strings.Compare(aggr, "empty") != 0 { // 聚合函数不为空，列名应该是聚合函数的名字
			columns = append(columns, "ts")
			columns = append(columns, "tbname")

			// ts tbname avg(usage_user) avg(usage_nice)
			fields := strings.Split(sf, ",")
			fields = fields[1:]
			for _, f := range fields {
				idx := strings.Index(f, "[") // "[" 前面的字符串是列名，后面的是数据类型
				columnName := f[:idx]
				columns = append(columns, fmt.Sprintf("%s(%s)", aggr, columnName))
			}
		} else { // 没有聚合函数，用正常的列名
			fields := strings.Split(sf, ",") // time[int64],randtag[string]...
			for _, f := range fields {
				idx := strings.Index(f, "[") // "[" 前面的字符串是列名，后面的是数据类型
				columnName := f[:idx]
				columns = append(columns, columnName)
			}
		}

		header := &wrapper.RowsHeader{
			ColNames:  columns,
			ColTypes:  ColumnTypeFromDatatype(datatypes),
			ColLength: ColumnLength(datatypes),
		}
		data := &async.ExecResult{
			AffectedRows: 0,
			FieldCount:   len(columns),
			Header:       header,
			Data:         valuess[i],
		}

		resp = append(resp, data)
	}

	return resp, flagNum, flagArr, timeRangeArr, tagArr
}

func ResponseToByteArrayWithParams(resp *async.ExecResult, datatypes []string, tags []string, metric string, partialSegment string) ([]byte, int64) {
	result := make([]byte, 0)
	dataBytes := make([]byte, 0)

	if ResponseIsEmpty(resp) {
		return nil, 0
	}

	singleSegments := GetSingleSegment(metric, partialSegment, tags)

	bytesPerLine := BytesPerLine(datatypes)

	curTag := resp.Data[0][1].(string)
	var curLines int = 0
	segIdx := 0
	for _, row := range resp.Data {

		tag := row[1].(string)
		if tag != curTag {
			curTag = tag
			bytesPerSeries, _ := Int64ToByteArray(int64(bytesPerLine * curLines))
			curLines = 0
			/* 存入一张表的 semantic segment 和表内所有数据的总字节数 */
			result = append(result, []byte(singleSegments[segIdx])...)
			result = append(result, []byte(" ")...)
			result = append(result, bytesPerSeries...)
			result = append(result, dataBytes...)

			dataBytes = nil
			segIdx++
		}
		curLines++
		/* 数据转换成字节数组，存入 */
		for j, v := range row {
			if j == 1 { // 不传入 tag 列
				continue
			}
			datatype := datatypes[j]
			if strings.Contains(partialSegment, "mean") && j > 1 && datatype == "int64" {
				datatype = "float64"
			}
			tmpBytes := InterfaceToByteArray(j, datatype, v)
			dataBytes = append(dataBytes, tmpBytes...)
		}

	}
	// 最后一张表
	bytesPerSeries, _ := Int64ToByteArray(int64(bytesPerLine * curLines))
	result = append(result, []byte(singleSegments[segIdx])...)
	result = append(result, []byte(" ")...)
	result = append(result, bytesPerSeries...)
	result = append(result, dataBytes...)
	segIdx++

	return result, int64(segIdx)
}

func RemainResponseToByteArrayWithParams(resp *async.ExecResult, datatypes []string, tags []string, metric string, partialSegment string) ([]byte, int64, map[string][][]driver.Value) {
	result := make([]byte, 0)
	dataBytes := make([]byte, 0)

	subTable := make(map[string][][]driver.Value)

	if ResponseIsEmpty(resp) {
		return nil, 0, nil
	}

	singleSegments := GetSingleSegment(metric, partialSegment, tags)

	bytesPerLine := BytesPerLine(datatypes)
	for _, row := range resp.Data {
		tag := row[1].(string)
		if _, ok := subTable[tag]; !ok {
			subTable[tag] = make([][]driver.Value, 0)
		}
		subTable[tag] = append(subTable[tag], row)
	}

	for _, seg := range singleSegments {
		sIdx := strings.Index(seg, "=")
		eIdx := strings.Index(seg, ")")
		tag := seg[sIdx+1 : eIdx]
		values := subTable[tag]
		bytesPerSeries, _ := Int64ToByteArray(int64(bytesPerLine * len(values)))

		/* 存入一张表的 semantic segment 和表内所有数据的总字节数 */
		result = append(result, []byte(seg)...)
		result = append(result, []byte(" ")...)
		result = append(result, bytesPerSeries...)
		result = append(result, dataBytes...)

		mean := strings.Contains(partialSegment, "mean")
		for _, row := range values {
			/* 数据转换成字节数组，存入 */
			for j, v := range row {
				if j == 1 { // 不传入 tag 列
					continue
				}
				datatype := datatypes[j]
				if mean && j > 1 && datatype == "int64" {
					datatype = "float64"
				}
				tmpBytes := InterfaceToByteArray(j, datatype, v)
				dataBytes = append(dataBytes, tmpBytes...)
			}
		}

	}

	return result, int64(len(singleSegments)), subTable
}

func InterfaceToByteArray(index int, datatype string, value interface{}) []byte {
	result := make([]byte, 0)

	/* 根据所在列的数据类型处理数据 */
	switch datatype {
	case "bool":
		if value != nil { // 值不为空
			bv, ok := value.(bool)
			if !ok {
				log.Fatal(fmt.Errorf("{}interface fail to convert to bool"))
			} else {
				bBytes, err := BoolToByteArray(bv)
				if err != nil {
					log.Fatal(fmt.Errorf(err.Error()))
				} else {
					result = append(result, bBytes...)
				}
			}
		} else { // 值为空
			bBytes, _ := BoolToByteArray(false)
			result = append(result, bBytes...)
		}
		break
	case "int64":
		if value != nil {
			if reflect.ValueOf(value).CanInt() {
				tmpInt := reflect.ValueOf(value).Int()
				if index == 0 {
					tmpInt /= 1000
				}
				iBytes, err := Int64ToByteArray(tmpInt)
				if err != nil {
					log.Fatal(fmt.Errorf(err.Error()))
				} else {
					result = append(result, iBytes...)
				}
			} else {
				log.Fatal(fmt.Errorf("{}interface fail to convert to int64"))
			}
		} else { // 值为空时设置默认值
			iBytes, _ := Int64ToByteArray(0)
			result = append(result, iBytes...)
		}
		break
	case "float64":
		if value != nil {
			if reflect.ValueOf(value).CanFloat() {
				tmpFloat := reflect.ValueOf(value).Float()
				fBytes, err := Float64ToByteArray(tmpFloat)
				if err != nil {
					log.Fatal(fmt.Errorf(err.Error()))
				} else {
					result = append(result, fBytes...)
				}
			} else {
				log.Fatal(fmt.Errorf("{}interface fail to convert to float64"))
			}
		} else {
			fBytes, _ := Float64ToByteArray(0)
			result = append(result, fBytes...)
		}
		break
	default: // string
		if value != nil {
			sv, ok := value.(string)
			if !ok {
				log.Fatal(fmt.Errorf("{}interface fail to convert to string"))
			} else {
				sBytes := StringToByteArray(sv)
				result = append(result, sBytes...)
			}
		} else {
			sBytes := StringToByteArray(string(byte(0))) // 空字符串
			result = append(result, sBytes...)
		}
		break
	}
	return result
}

func BytesPerLine(datatypes []string) int {
	bytesPerLine := 0
	for i, d := range datatypes {
		switch d {
		case "bool":
			bytesPerLine += 1
			break
		case "int64":
			bytesPerLine += 8
			break
		case "float64":
			bytesPerLine += 8
			break
		case "string":
			if i != 1 {
				bytesPerLine += STRINGBYTELENGTH
			}
			break
		default:
			bytesPerLine += 8
			break
		}
	}
	return bytesPerLine
}

func ColumnLength(datatypes []string) []int64 {
	bytesPerLine := make([]int64, 0)
	for _, d := range datatypes {
		switch d {
		case "bool":
			bytesPerLine = append(bytesPerLine, 1)
			break
		case "int64":
			bytesPerLine = append(bytesPerLine, 8)
			break
		case "float64":
			bytesPerLine = append(bytesPerLine, 8)
			break
		case "string":
			bytesPerLine = append(bytesPerLine, STRINGBYTELENGTH)
			break
		default:
			bytesPerLine = append(bytesPerLine, 8)
			break
		}
	}
	return bytesPerLine
}

func BoolToByteArray(b bool) ([]byte, error) {
	bytesBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(bytesBuffer, binary.LittleEndian, &b)
	if err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}

func ByteArrayToBool(byteArray []byte) (bool, error) {
	if len(byteArray) != 1 {
		return false, errors.New("incorrect length of byte array, can not convert []byte to bool\n")
	}
	var b bool
	byteBuffer := bytes.NewBuffer(byteArray)
	err := binary.Read(byteBuffer, binary.LittleEndian, &b)
	if err != nil {
		return false, err
	}
	return b, nil
}

func StringToByteArray(str string) []byte {
	byteArray := make([]byte, 0, STRINGBYTELENGTH)
	byteStr := []byte(str)
	if len(byteStr) > STRINGBYTELENGTH {
		return byteStr[:STRINGBYTELENGTH]
	}
	byteArray = append(byteArray, byteStr...)
	for i := 0; i < cap(byteArray)-len(byteStr); i++ {
		byteArray = append(byteArray, 0)
	}

	return byteArray
}

func ByteArrayToString(byteArray []byte) string {
	byteArray = bytes.Trim(byteArray, string(byte(0)))
	str := string(byteArray)
	return str
}

func Int64ToByteArray(number int64) ([]byte, error) {
	byteBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(byteBuffer, binary.LittleEndian, &number)
	if err != nil {
		return nil, err
	}
	return byteBuffer.Bytes(), nil
}

func ByteArrayToInt64(byteArray []byte) (int64, error) {
	if len(byteArray) != 8 {
		return 0, errors.New("incorrect length of byte array, can not convert []byte to int64\n")
	}
	var number int64
	byteBuffer := bytes.NewBuffer(byteArray)
	err := binary.Read(byteBuffer, binary.LittleEndian, &number)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func Float64ToByteArray(number float64) ([]byte, error) {
	byteBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(byteBuffer, binary.LittleEndian, &number)
	if err != nil {
		return nil, err
	}
	return byteBuffer.Bytes(), nil
}

func ByteArrayToFloat64(byteArray []byte) (float64, error) {
	if len(byteArray) != 8 {
		return 0, errors.New("incorrect length of byte array, can not canvert []byte to float64\n")
	}
	var number float64
	byteBuffer := bytes.NewBuffer(byteArray)
	err := binary.Read(byteBuffer, binary.LittleEndian, &number)
	if err != nil {
		return 0.0, err
	}
	return number, nil
}

func ResultToString(data *async.ExecResult) string {
	var result string

	if ResponseIsEmpty(data) {
		return "empty response"
	}

	colTypes := DataTypeFromColumn(data.Header.ColTypes)
	for i, colName := range data.Header.ColNames {
		result += fmt.Sprintf("%s[%s]\t", colName, colTypes[i])
	}
	result += "\n"

	for _, row := range data.Data {
		for j, col := range row {
			if col == nil {
				result += "-"
			} else if colTypes[j] == "string" {
				str := col.(string)
				result += str
			} else if reflect.ValueOf(col).CanInt() {
				tmpInt := reflect.ValueOf(col).Int()
				str := strconv.FormatInt(tmpInt, 10)
				result += str
			} else if reflect.ValueOf(col).CanFloat() {
				tmpFloat := reflect.ValueOf(col).Float()
				str := strconv.FormatFloat(tmpFloat, 'g', -1, 64)
				result += str
			} else if colTypes[j] == "bool" {
				str := ""
				if reflect.ValueOf(col).Bool() {
					str = "1"
				} else {
					str = "0"
				}
				result += str
			}
			result += "\t"
		}

		result += "\n"
	}

	result += "end" //标志响应转换结束
	return result
}
