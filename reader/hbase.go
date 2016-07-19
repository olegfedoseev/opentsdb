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

	result := make([]string, 0)
	for key, val := range metrics {
		log.Printf("%v -> %v", key, val)
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

func (client *Client) GetDatapoints(start, end time.Time, metric string) (opentsdb.DataPoints, error) {
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

	log.Printf("prefixStart: %d %#v", int32(start.Unix()), prefixStart)
	log.Printf("prefixEnd: %d %#v", int32(end.Unix()), prefixEnd)

	t := time.Now()
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
	log.Printf("time: %v", time.Since(t))

	t = time.Now()
	result := make(opentsdb.DataPoints, 0)
	for idx, row := range scanRsp {
		for _, cell := range row.Cells {
			// isFloat(cell.Qualifier, 0)
			//value := bytesToIntValue(cell.Value)
			//value := bytesToFloatValue(cell.Value)
			//flags := getFlagsFromQualifier(qualifier, 0)
			//val := getOffsetFromQualifier(cell.Qualifier, 0)

			// log.Printf("len(cell.Qualifier): %d: float? %v, int? %v, valLen: %v, len(cell.Value): %v",
			// 	len(cell.Qualifier), isFloat(cell.Qualifier, 0), isInteger(cell.Qualifier, 0),
			// 	getValueLengthFromQualifier(cell.Qualifier, 0),
			// 	len(cell.Value),
			// )

			tags := make(opentsdb.Tags)
			tagsRaw := cell.Row[7:]
			tagCnt := len(tagsRaw) / 6
			for i := 0; i < tagCnt; i++ {
				key := client.getTagkByUID(tagsRaw[i*6 : i*6+3])
				val := client.getTagvByUID(tagsRaw[i*6+3 : i*6+6])

				tags[key] = val
			}

			ts := int64(bytesToTimestamp(cell.Row[3:7]) + getOffsetFromQualifier(cell.Qualifier, 0))

			if len(cell.Qualifier) > 4 && len(cell.Qualifier)%2 == 0 {
				var valIdx int32
				for i := 0; i < len(cell.Qualifier); i += 2 {
					qualifier := extractQualifier(cell.Qualifier, int32(i))
					valLen := int32(getValueLengthFromQualifier(cell.Qualifier, int32(i)))
					if inMilliseconds(cell.Qualifier[int32(i)]) {
						i += 2
						log.Println("inMilliseconds")
					}

					val := cell.Value[valIdx : valIdx+valLen]
					valIdx += valLen

					log.Printf("qualifier: %#v, idx: %v, len: %v, offset: %v, val: %#v", qualifier, valIdx, valLen, int32(i), val)

					if isFloat(cell.Qualifier, int32(i)) {
						log.Printf("%d Float!!!! %2.6f", ts, bytesToFloatValue(val))
					} else {
						log.Printf("%d else INT!!!! %v", ts, bytesToIntValue(val))
					}
				}
				continue
			}

			valLen := getValueLengthFromQualifier(cell.Qualifier, 0)
			var val float32
			if isFloat(cell.Qualifier, 0) {
				val = bytesToFloatValue(cell.Value[0:valLen])
			} else {
				val = float32(bytesToIntValue(cell.Value[0:valLen]))
			}

			dp := opentsdb.DataPoint{
				Metric:    client.getMetricByUID(cell.Row[0:3]),
				Timestamp: ts,
				Value:     val,
				Tags:      tags,
			}

			//log.Printf("getValueLengthFromQualifier: %v", getValueLengthFromQualifier(cell.Qualifier, 0))

			// //"script":"Job_Api_Categories__find",
			// if val, ok := tags["script"]; !ok || val != "Job_Api_Categories__find" {
			// 	continue
			// }

			// if val, ok := tags["user"]; !ok || val != "auth" {
			// 	continue
			// }

			// // if val, ok := tags["is_ajax"]; !ok || val != "no" {
			// // 	continue
			// // }

			// if val, ok := tags["status"]; !ok || val != "200" {
			// 	continue
			// }

			// if val, ok := tags["region"]; !ok || val != "54" {
			// 	continue
			// }
			result = append(result, &dp)

			// 1421647200 - 1421629200 = 18000

			//cell.
			fmt.Printf("[%d] %#v\n", idx, dp)
			//fmt.Printf("[%d] %v\n", idx, cell.CellType)
			//log.Printf("cell.Qualifier: %#v", cell.Qualifier)

			// if isInteger(cell.Qualifier, 0) {
			// 	log.Printf("%d INT!!!! %v", int64(bytesToTimestamp(cell.Row[3:7])), bytesToIntValue(cell.Value[0:valLen]))
			// } else {
			// 	log.Printf("%d else Float!!!! %2.6f", int64(bytesToTimestamp(cell.Row[3:7])), bytesToFloatValue(cell.Value[0:valLen]))
			// }

			// if isFloat(cell.Qualifier, 0) {
			// 	log.Printf("%d Float!!!! %2.6f", int64(bytesToTimestamp(cell.Row[3:7])), bytesToFloatValue(cell.Value[0:valLen]))
			// } else {
			// 	log.Printf("%d else INT!!!! %v", int64(bytesToTimestamp(cell.Row[3:7])), bytesToIntValue(cell.Value[0:valLen]))
			// }
		}
		// if idx > 10 {
		// 	break
		// }
	}
	log.Printf("range: %v", time.Since(t))
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
	//log.Printf("getFlagsFromQualifier(qualifier, offset)&FLAG_FLOAT: %#v, %v", qualifier, getFlagsFromQualifier(qualifier, offset)&FLAG_FLOAT)
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
	//buf := bytes.NewBuffer(value)
	switch len(value) {
	case 4:
		return int32(binary.BigEndian.Uint32(value))
	case 2:
		return int32(binary.BigEndian.Uint16(value))
	}
	return 0
	// var result int32
	// //log.Printf("int value %d %#v: %v", len(value), value, int32(binary.BigEndian.Uint32(value)))
	// if err := binary.Read(buf, binary.BigEndian, &result); err != nil {
	// 	log.Printf("Failer to read int value %#v: %v", value, err)
	// 	return 0
	// }
	// return result
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

/**
* Returns whether or not this is a floating value that needs to be fixed.
* <p>
* OpenTSDB used to encode all floating point values as `float' (4 bytes)
* but actually store them on 8 bytes, with 4 leading 0 bytes, and flags
* correctly stating the value was on 4 bytes.
* (from CompactionQueue)
* @param flags The least significant byte of a qualifier.
* @param value The value that may need to be corrected.
 */
func floatingPointValueToFix(flags byte, value []byte) bool {
	// We need a floating point value. That pretends to be on 4 bytes. But is actually using 8 bytes.
	return (flags&FLAG_FLOAT) != 0 && (flags&LENGTH_MASK) == 0x3 && len(value) == 8
}

/**
* Returns a corrected value if this is a floating point value to fix.
* <p>
* OpenTSDB used to encode all floating point values as `float' (4 bytes)
* but actually store them on 8 bytes, with 4 leading 0 bytes, and flags
* correctly stating the value was on 4 bytes.
* <p>
* This function detects such values and returns a corrected value, without
* the 4 leading zeros.  Otherwise it returns the value unchanged.
* (from CompactionQueue)
* @param flags The least significant byte of a qualifier.
* @param value The value that may need to be corrected.
* @throws IllegalDataException if the value is malformed.
 */
func fixFloatingPointValue(flags byte, value []byte) []byte {
	if !floatingPointValueToFix(flags, value) {
		return value
	}

	// The first 4 bytes should really be zeros.
	if value[0] == 0 && value[1] == 0 && value[2] == 0 && value[3] == 0 {
		// Just keep the last 4 bytes.
		return value[4:8]
	}

	// Very unlikely.
	return []byte{}
	// throw new IllegalDataException("Corrupted floating point value: "
	// 			+ Arrays.toString(value) + " flags=0x" + Integer.toHexString(flags)
	// 			+ " -- first 4 bytes are expected to be zeros.")
}

/**
* Fix the flags inside the last byte of a qualifier.
* <p>
* OpenTSDB used to not rely on the size recorded in the flags being
* correct, and so for a long time it was setting the wrong size for
* floating point values (pretending they were encoded on 8 bytes when
* in fact they were on 4).  So overwrite these bits here to make sure
* they're correct now, because once they're compacted it's going to
* be quite hard to tell if the flags are right or wrong, and we need
* them to be correct to easily decode the values.
* @param flags The least significant byte of a qualifier.
* @param val_len The number of bytes in the value of this qualifier.
* @return The least significant byte of the qualifier with correct flags.
 */
func fixQualifierFlags(flags byte, val_len int32) byte {
	// Explanation:
	//   (1) Take the last byte of the qualifier.
	//   (2) Zero out all the flag bits but one.
	//       The one we keep is the type (floating point vs integer value).
	//   (3) Set the length properly based on the value we have.
	return 0 //(byte) ((flags & ~(FLAGS_MASK >>> 1)) | (val_len - 1))
	//              ^^^^^   ^^^^^^^^^^^^^^^^^^^^^^^^^    ^^^^^^^^^^^^^
	//               (1)               (2)                    (3)
}

/**
* Breaks down all the values in a row into individual {@link Cell}s sorted on
* the qualifier. Columns with non data-point data will be discarded.
* <b>Note:</b> This method does not account for duplicate timestamps in
* qualifiers.
* @param row An array of data row columns to parse
* @param estimated_nvalues Estimate of the number of values to compact.
* Used to pre-allocate a collection of the right size, so it's better to
* overshoot a bit to avoid re-allocations.
* @return An array list of data point {@link Cell} objects. The list may be
* empty if the row did not contain a data point.
* @throws IllegalDataException if one of the cells cannot be read because
* it's corrupted or in a format we don't understand.
* @since 2.0
 */
// func extractDataPoints(ArrayList<KeyValue> row, int estimated_nvalues) DataPoints {
// 	//ArrayList<Cell> cells = new ArrayList<Cell>(estimated_nvalues);
// 	//for (KeyValue kv : row) {
//   		var qual []byte = kv.qualifier()
//   		var qualLen int32 = len(qual)
//   		var val []byte = kv.value()

//   		if qualLen % 2 != 0 {
//   			// skip a non data point column
//   			continue

//   		} else if qualLen == 2 {  // Single-value cell.
//   			// Maybe we need to fix the flags in the qualifier.
//   			var actual_val []byte = fixFloatingPointValue(qual[1], val)
//   			var q byte = fixQualifierFlags(qual[1], len(actual_val))

//   			var actual_qual []byte = qual
//     		if q != qual[1] {  // We need to fix the qualifier.
//       			actual_qual = []byte{qual[0], q}  // So make a copy.
//     		}

//     		// Cell cell = new Cell(actual_qual, actual_val);
//     		// cells.add(cell);
//     		continue

//   		} else if (qualLen == 4 && inMilliseconds(qual[0])) {
//     		// since ms support is new, there's nothing to fix
//     		//Cell cell = new Cell(qual, val);
//     		//cells.add(cell);
//     		continue
//   		}

// 	  	// Now break it down into Cells.
// 	  	int val_idx = 0;
// 	  	//try {
// 	    	for (int i = 0; i < qualLen; i += 2) {
// 	      		var q []byte = extractQualifier(qual, i)
// 	      		var vlen int32  = getValueLengthFromQualifier(qual, i)
// 	      		if inMilliseconds(qual[i]) {
// 	        		i += 2
// 	      		}

// 	      		//var v []byte = new byte[vlen]
// 	      		//System.arraycopy(val, val_idx, v, 0, vlen);
// 	      		val_idx += vlen
// 	      		//Cell cell = new Cell(q, v);
// 	      		//cells.add(cell);
// 	    	}
// 	  	// } catch (ArrayIndexOutOfBoundsException e) {
// 	   //  	throw new IllegalDataException("Corrupted value: couldn't break down"
// 		  //       + " into individual values (consumed " + val_idx + " bytes, but was"
// 		  //       + " expecting to consume " + (val.length - 1) + "): " + kv
// 		  //       + ", cells so far: " + cells);
// 	  	// }

// 	  	// Check we consumed all the bytes of the value.  Remember the last byte
// 	  	// is metadata, so it's normal that we didn't consume it.
// 	  	if (val_idx != val.length - 1) {
// 	    	// throw new IllegalDataException("Corrupted value: couldn't break down"
// 		    //   + " into individual values (consumed " + val_idx + " bytes, but was"
// 	    	//   + " expecting to consume " + (val.length - 1) + "): " + kv
// 	     //  	+ ", cells so far: " + cells);
// 	  	}
// 	//}

// //	Collections.sort(cells);
// 	return cells
// }
