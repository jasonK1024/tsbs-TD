package tdengine_client

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/taosdata/tsbs/pkg/targets/tdengine/async"
	"log"
	"reflect"
	"strconv"
)

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
			if index == 0 { // 第一列的时间戳
				if timestamp, ok := value.(string); ok {
					tsi := TimeStringToInt64(timestamp)
					iBytes, err := Int64ToByteArray(tsi)
					if err != nil {
						log.Fatal(fmt.Errorf(err.Error()))
					} else {
						result = append(result, iBytes...)
					}
				} else if timestamp, ok := value.(json.Number); ok {
					jvi, err := timestamp.Int64()
					if err != nil {
						log.Fatal(fmt.Errorf(err.Error()))
					} else {
						iBytes, err := Int64ToByteArray(jvi)
						if err != nil {
							log.Fatal(fmt.Errorf(err.Error()))
						} else {
							result = append(result, iBytes...)
						}
					}
				} else {
					log.Fatal("timestamp fail to convert to []byte")
				}

			} else { // 除第一列以外的所有列
				jv, ok := value.(json.Number)
				if !ok {
					log.Fatal(fmt.Errorf("{}interface fail to convert to json.Number"))
				} else {
					jvi, err := jv.Int64()
					if err != nil {
						log.Fatal(fmt.Errorf(err.Error()))
					} else {
						iBytes, err := Int64ToByteArray(jvi)
						if err != nil {
							log.Fatal(fmt.Errorf(err.Error()))
						} else {
							result = append(result, iBytes...)
						}
					}
				}
			}
		} else { // 值为空时设置默认值
			iBytes, _ := Int64ToByteArray(0)
			result = append(result, iBytes...)
		}
		break
	case "float64":
		if value != nil {
			jv, ok := value.(json.Number)
			if !ok {
				log.Fatal(fmt.Errorf("{}interface fail to convert to json.Number"))
			} else {
				jvf, err := jv.Float64()
				if err != nil {
					log.Fatal(fmt.Errorf(err.Error()))
				} else {
					fBytes, err := Float64ToByteArray(jvf)
					if err != nil {
						log.Fatal(fmt.Errorf(err.Error()))
					} else {
						result = append(result, fBytes...)
					}
				}
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
	for _, d := range datatypes {
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
		default:
			bytesPerLine += STRINGBYTELENGTH
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

func QueryResultToString(data *async.ExecResult) string {
	var result string

	for _, colName := range data.Header.ColNames {
		result += fmt.Sprintf("%s\t", colName)
	}
	result += "\n"

	for _, row := range data.Data {
		for _, col := range row {
			if col == nil {
				result += "-"
			} else if str, ok := col.(string); ok {
				result += str
			} else if reflect.ValueOf(col).CanInt() {
				tmpInt := reflect.ValueOf(col).Int()
				str := strconv.FormatInt(tmpInt, 10)
				result += str
			} else if reflect.ValueOf(col).CanFloat() {
				tmpFloat := reflect.ValueOf(col).Float()
				str := strconv.FormatFloat(tmpFloat, 'g', -1, 64)
				result += str
			} else if b, ok := col.(bool); ok {
				if b {
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
