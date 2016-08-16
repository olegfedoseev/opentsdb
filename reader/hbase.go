package reader

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/olegfedoseev/opentsdb"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
)

/*

http://tsdb.kronos.d:4242/#

start=2016/01/21-14:30:26
end=2016/01/21-15:08:14
m=zimsum:10m-avg-none:php.requests.ngs.ru.rps{script=*,type=*}

*/

type Client struct {
	hbase gohbase.Client

	metrics map[string]string
	tagk    map[string]string
	tagv    map[string]string
}

const (
	// Number of LSBs in time_deltas reserved for flags.
	FLAG_BITS uint16 = 4

	// Number of LSBs in time_deltas reserved for flags.
	MS_FLAG_BITS uint16 = 6

	// When this bit is set, the value is a floating point value.
	// Otherwise it's an integer value.
	FLAG_FLOAT byte = 0x8

	// Mask to select the size of a value from the qualifier.
	LENGTH_MASK byte = 0x7

	// Mask for the millisecond qualifier flag
	MS_BYTE_FLAG byte = 0xF0

	// Mask to select all the FLAG_BITS
	FLAGS_MASK byte = FLAG_FLOAT | LENGTH_MASK

	UID_TABLE string = "tsdb-uid"
)

// New creates new hbase client and prepare internal caches for UIDs
func New(dsn string) *Client {
	return &Client{
		hbase: gohbase.NewClient(dsn),

		metrics: make(map[string]string),
		tagk:    make(map[string]string),
		tagv:    make(map[string]string),
	}
}

func (client *Client) FindMetrics(prefix string) ([]string, error) {
	metrics, err := client.getUID("metrics", prefix)
	if err != nil {
		return nil, err
	}

	// TODO: maybe map[string][]byte (php.requests.zarplata.ru.p25 -> [0 68 171])
	result := make([]string, 0)
	for key, _ := range metrics {
		result = append(result, key)
	}
	sort.Strings(result)
	return result, nil
}

func (client *Client) GetTagsForMetrics(start, end time.Time, metric string) ([]string, error) {
	metrics, err := client.getUID("metrics", metric)
	if err != nil {
		return nil, fmt.Errorf("failed to get UID for %s: %v", metric, err)
	}
	if len(metrics) != 1 {
		return nil, fmt.Errorf("expect get one UID for %s, got %d", metric, len(metrics))
	}

	prefixStart := []byte{}
	prefixEnd := []byte{}
	prefixStart = append(prefixStart, metrics[metric]...)
	prefixEnd = append(prefixEnd, metrics[metric]...)

	prefixStart = append(prefixStart, timestampToBytes(int32(start.Unix())-1)...)
	prefixEnd = append(prefixEnd, timestampToBytes(int32(end.Unix())+1)...)

	scanRequest, err := hrpc.NewScanRange(
		context.Background(),
		[]byte("tsdb"),
		prefixStart,
		prefixEnd,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create scan: %v", err)
	}
	scanRsp, err := client.hbase.Scan(scanRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to scan: %v", err)
	}

	tags := make(opentsdb.Tags)
	for _, row := range scanRsp {
		for _, cell := range row.Cells {
			tagsRaw := cell.Row[7:]
			tagCnt := len(tagsRaw) / 6
			for i := 0; i < tagCnt; i++ {
				key := client.getTagkByUID(tagsRaw[i*6 : i*6+3])
				val := client.getTagvByUID(tagsRaw[i*6+3 : i*6+6])
				tags[key] = val
			}
		}
	}

	result := make([]string, len(tags))
	for key, _ := range tags {
		if key != "" {
			result = append(result, key)
		}
	}
	return result, nil
}

func (client *Client) getUID(uidType, uidName string) (map[string][]byte, error) {
	scanRequest, err := hrpc.NewScanStr(
		context.Background(),
		UID_TABLE,
		hrpc.Filters(
			filter.NewList(
				filter.MustPassAll,
				filter.NewQualifierFilter(
					filter.NewCompareFilter(
						filter.Equal,
						filter.NewBinaryComparator(
							filter.NewByteArrayComparable([]byte(uidType))),
					),
				),
				filter.NewPrefixFilter([]byte(uidName)),
			),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create scan: %v", err)
	}

	scanRsp, err := client.hbase.Scan(scanRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to scan: %v", err)
	}

	result := make(map[string][]byte)
	for _, row := range scanRsp {
		for _, cell := range row.Cells {
			result[string(cell.Row)] = cell.Value
		}
	}

	return result, nil
}

func (client *Client) getByUID(uidType, uidName []byte) (string, error) {
	scanRequest, err := hrpc.NewGet(context.Background(), []byte("tsdb-uid"), uidName)

	if err != nil {
		return "", fmt.Errorf("failed to create scan: %v", err)
	}

	scanRsp, err := client.hbase.Get(scanRequest)
	if err != nil {
		return "", fmt.Errorf("failed to scan: %v", err)
	}

	for _, cell := range scanRsp.Cells {
		if string(uidType) == string(cell.Qualifier) {
			return string(cell.Value), nil
		}
	}
	return "", fmt.Errorf("failed to find")
}

func (client *Client) GetDatapoints(start, end int32, metric string, tags opentsdb.Tags) (opentsdb.DataPoints, error) {
	metrics, err := client.getUID("metrics", metric)
	if err != nil {
		return nil, fmt.Errorf("failed to get UID for %s: %v", metric, err)
	}
	if len(metrics) != 1 {
		return nil, fmt.Errorf("expect get one UID for %s, got %d", metric, len(metrics))
	}

	prefixStart := []byte{}
	prefixEnd := []byte{}
	prefixStart = append(prefixStart, metrics[metric]...)
	prefixEnd = append(prefixEnd, metrics[metric]...)

	// round down to nearest hour
	prefixStart = append(prefixStart, timestampToBytes(start-start%3600)...)
	// round up to next hour
	prefixEnd = append(prefixEnd, timestampToBytes(end-end%3600+3601)...)

	ctx, _ := context.WithTimeout(context.Background(), 90*time.Second)
	scanRequest, err := hrpc.NewScanRange(
		ctx,
		[]byte("tsdb"),
		prefixStart,
		prefixEnd,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create scan: %v", err)
	}

	scanRsp, err := client.hbase.Scan(scanRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to scan: %v", err)
	}

	result := make(opentsdb.DataPoints, 0)
	for _, row := range scanRsp {
		for _, cell := range row.Cells {
			// TODO: verify somehow
			if len(cell.Qualifier) > 4 && len(cell.Qualifier)%2 == 0 {
				var valIdx int32
				for i := 0; i < len(cell.Qualifier); i += 2 {
					valLen := int32(getValueLengthFromQualifier(cell.Qualifier, int32(i)))
					if inMilliseconds(cell.Qualifier[int32(i)]) {
						i += 2
					}
					valIdx += valLen
				}
				fmt.Printf("continue! (%v)\n", cell.Qualifier)
				continue
			}

			dpTags := make(opentsdb.Tags)
			tagsRaw := cell.Row[7:]
			tagsCnt := len(tagsRaw) / 6
			for i := 0; i < tagsCnt; i++ {
				key := client.getTagkByUID(tagsRaw[i*6 : i*6+3])
				val := client.getTagvByUID(tagsRaw[i*6+3 : i*6+6])
				dpTags[key] = val
			}

			skip := false
			for k, v := range tags {
				if val, ok := dpTags[k]; !ok || val != v {
					skip = true
					break
				}
			}
			if skip {
				continue
			}

			valLen := getValueLengthFromQualifier(cell.Qualifier, 0)
			var val float32
			if isFloat(cell.Qualifier, 0) {
				val = bytesToFloatValue(cell.Value[0:valLen])
			} else {
				val = float32(bytesToIntValue(cell.Value[0:valLen]))
			}

			ts := bytesToTimestamp(cell.Row[3:7]) + getOffsetFromQualifier(cell.Qualifier, 0)
			if ts > end || ts < start {
				continue
			}

			dp := opentsdb.DataPoint{
				Metric:    client.getMetricByUID(cell.Row[0:3]),
				Timestamp: int64(ts),
				Value:     val,
				Tags:      dpTags,
			}
			result = append(result, &dp)
		}
	}
	return result, nil
}

func (client *Client) getMetricByUID(uid []byte) string {
	if val, ok := client.metrics[string(uid)]; ok {
		return val
	}
	val, err := client.getByUID([]byte("metrics"), uid)
	if err != nil {
		return ""
	}
	client.metrics[string(uid)] = val
	return val
}

func (client *Client) getTagkByUID(uid []byte) string {
	if val, ok := client.tagk[string(uid)]; ok {
		return val
	}
	val, err := client.getByUID([]byte("tagk"), uid)
	if err != nil {
		return ""
	}

	client.tagk[string(uid)] = val
	return val
}

func (client *Client) getTagvByUID(uid []byte) string {
	if val, ok := client.tagv[string(uid)]; ok {
		return val
	}
	val, err := client.getByUID([]byte("tagv"), uid)
	if err != nil {
		return ""
	}
	client.tagv[string(uid)] = val
	return val
}

/*
id Column Family
Row Key - This will be the string assigned to the UID. E.g. for a metric we may
have a value of sys.cpu.user or for a tag value it may be 42.

Column Qualifiers - One of the standard column types above.

Column Value - An unsigned integer encoded on 3 bytes by default reflecting
the UID assigned to the string for the column type. If the UID length has been
changed in the source code, the width may vary.
*/

/**
 * Returns the offset in milliseconds from the row base timestamp from a data
 * point qualifier at the given offset (for compacted columns)
 * @param qualifier The qualifier to parse
 * @param offset An offset within the byte array
 * @return The offset in milliseconds from the base time
 */
func getOffsetFromQualifier(qualifier []byte, offset int32) int32 {
	if (qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG {
		return int32((binary.BigEndian.Uint32(qualifier[offset:offset+4])&0x0FFFFFC0)>>MS_FLAG_BITS) / 1000
	}
	return int32((binary.BigEndian.Uint16(qualifier[offset:offset+2]) & 0xFFFF) >> FLAG_BITS)
}

func getFlagsFromQualifier(qualifier []byte, offset int32) byte {
	if (qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG {
		return qualifier[offset+3] & FLAGS_MASK
	}
	return qualifier[offset+1] & FLAGS_MASK
}

/**
 * Parses the qualifier to determine if the data is a floating point value.
 * 4 bytes == Float, 8 bytes == Double
 * @param qualifier The qualifier to parse
 * @param offset An offset within the byte array
 * @return True if the encoded data is a floating point value
 * @throws IllegalArgumentException if the qualifier is null or the offset falls
 * outside of the qualifier array
 * @since 2.1
 */
func isFloat(qualifier []byte, offset int32) bool {
	return getFlagsFromQualifier(qualifier, offset)&FLAG_FLOAT != 0x00
}

func isInteger(qualifier []byte, offset int32) bool {
	return getFlagsFromQualifier(qualifier, offset)&FLAG_FLOAT == 0x00
}

func bytesToTimestamp(ts []byte) int32 {
	return int32(binary.BigEndian.Uint32(ts))
}

func bytesToFloatValue(value []byte) float32 {
	buf := bytes.NewBuffer(value)
	var result float32
	if err := binary.Read(buf, binary.BigEndian, &result); err != nil {
		log.Printf("Failed to read float value %#v: %v", value, err)
		return 0.0
	}
	return result
}

func bytesToIntValue(value []byte) int32 {
	switch len(value) {
	case 4:
		return int32(binary.BigEndian.Uint32(value))
	case 2:
		return int32(binary.BigEndian.Uint16(value))
	}
	return 0
}

/**
* Extracts the value of a cell containing a data point.
* @param value The contents of a cell in HBase.
* @param value_idx The offset inside {@code values} at which the value
* starts.
* @param flags The flags for this value.
* @return The value of the cell.
* @throws IllegalDataException if the data is malformed
 */
// func extractIntegerValue(values []byte, value_idx int32, flags byte) int32 {
// 	switch flags & LENGTH_MASK {
// 	case 7:
// 		return Bytes.getLong(values, value_idx)
// 	case 3:
// 		return Bytes.getInt(values, value_idx)
// 	case 1:
// 		return Bytes.getShort(values, value_idx)
// 	case 0:
// 		return values[value_idx]
// 	}
// 	//throw new IllegalDataException("Integer value @ " + value_idx + " not on 8/4/2/1 bytes in " + Arrays.toString(values));
// 	return 0
// }

/**
* Extracts the value of a cell containing a data point.
* @param value The contents of a cell in HBase.
* @param value_idx The offset inside {@code values} at which the value
* starts.
* @param flags The flags for this value.
* @return The value of the cell.
* @throws IllegalDataException if the data is malformed
 */
// func extractFloatingPointValue(values []byte, value_idx int32, flags byte) float32 {
// 	switch flags & LENGTH_MASK {
// 	case 7:
// 		return Double.longBitsToDouble(Bytes.getLong(values, value_idx))
// 	case 3:
// 		return Float.intBitsToFloat(Bytes.getInt(values, value_idx))
// 	}
// 	// throw new IllegalDataException("Floating point value @ " + value_idx
// 	//                               + " not on 8 or 4 bytes in "
// 	//                               + Arrays.toString(values));
// 	return 0.0
// }

func timestampToBytes(ts int32) []byte {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, ts); err != nil {
		return []byte{}
	}
	return buf.Bytes()
}

/**
* Returns the length of the value, in bytes, parsed from the qualifier
* @param qualifier The qualifier to parse
* @param offset An offset within the byte array
* @return The length of the value in bytes, from 1 to 8.
* @throws IllegalArgumentException if the qualifier is null or the offset falls
* outside of the qualifier array
* @since 2.0
 */
func getValueLengthFromQualifier(qualifier []byte, offset int32) byte {
	//validateQualifier(qualifier, offset);
	if (qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG {
		return (byte)(uint16(qualifier[offset+3]&LENGTH_MASK) + 1)
	}
	return (byte)(uint16(qualifier[offset+1]&LENGTH_MASK) + 1)
}

/**
* Extracts the 2 or 4 byte qualifier from a compacted byte array
* @param qualifier The qualifier to parse
* @param offset An offset within the byte array
* @return A byte array with only the requested qualifier
* @throws IllegalArgumentException if the qualifier is null or the offset falls
* outside of the qualifier array
* @since 2.0
 */
func extractQualifier(qualifier []byte, offset int32) []byte {
	//validateQualifier(qualifier, offset);
	if (qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG {
		return qualifier[offset : offset+4]
	}
	return qualifier[offset : offset+2]
}

/**
* Returns the length, in bytes, of the qualifier: 2 or 4 bytes
* @param qualifier The qualifier to parse
* @param offset An offset within the byte array
* @return The length of the qualifier in bytes
* @throws IllegalArgumentException if the qualifier is null or the offset falls
* outside of the qualifier array
* @since 2.0
 */
func getQualifierLength(qualifier []byte, offset int32) uint16 {
	//validateQualifier(qualifier, offset);
	if (qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG {
		// if (offset + 4) > len(qualifier) {
		// 	//throw new IllegalArgumentException("Detected a millisecond flag but qualifier length is too short")
		// }
		return 4
	}
	// if (offset + 2) > len(qualifier) {
	// 	//throw new IllegalArgumentException("Qualifier length is too short")
	// }
	return 2
}

/**
* Determines if the qualifier is in milliseconds or not
* @param qualifier The first byte of a qualifier
* @return True if the qualifier is in milliseconds, false if not
* @since 2.0
 */
func inMilliseconds(qualifier byte) bool {
	return (qualifier & MS_BYTE_FLAG) == MS_BYTE_FLAG
}
