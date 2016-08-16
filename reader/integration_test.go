// +build integration
package reader

import (
	"fmt"
	"log"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/olegfedoseev/opentsdb"
)

// go test -tags=integration

func TestFindMetrics(t *testing.T) {
	client := New("db1.kronos.d,db2.kronos.d,db3.kronos.d")

	result, err := client.FindMetrics("php.requests.zarplata.ru.p")
	if err != nil {
		log.Fatalf("Failed to get metrics: %v", err)
	}

	// Expected:
	// php.requests.zarplata.ru.p25
	// php.requests.zarplata.ru.p50
	// php.requests.zarplata.ru.p75
	// php.requests.zarplata.ru.p95
	if len(result) != 4 {
		t.Errorf("Expected 4 metrics, got %v", len(result))
		for _, metric := range result {
			fmt.Printf("%s\n", metric)
		}
	}
}

func TestReadAndCompareWithFullHour(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	client := New("db1.kronos.d,db2.kronos.d,db3.kronos.d")

	// Expected:
	// http://tsdb.kronos.d:4242/api/query/?start=2014/12/31-02:00:00&end=2014/12/31-05:00:00&m=mimmax:php.requests.realty.ngs.ru.rps{script=General.php,status=200,region=54}&json
	var start int32 = 1419966000
	var end int32 = 1419976800

	log.Printf("%v -> %v", start, end)

	tags := opentsdb.Tags{
		"script": "General.php",
		"region": "54",
		"status": "200",
	}

	dps, err := client.GetDatapoints(start, end, "php.requests.realty.ngs.ru.rps", tags)

	if err != nil {
		log.Fatalf("Failed to get metrics: %v", err)
	}

	expected := map[int64]float32{
		1419966000: 0.3483000099658966,
		1419966600: 0.3133000135421753,
		1419967200: 0.33169999718666077,
		1419967800: 0.2800000011920929,
		1419968400: 0.2732999920845032,
		1419969000: 0.26170000433921814,
		1419969600: 0.30169999599456787,
		1419970200: 0.23669999837875366,
		1419970800: 0.20499999821186066,
		1419971400: 0.29499998688697815,
		1419972000: 0.1899999976158142,
		1419972600: 0.22169999778270721,
		1419973200: 0.21170000731945038,
		1419973800: 0.19169999659061432,
		1419974400: 0.17669999599456787,
		1419975000: 0.19329999387264252,
		1419975600: 0.17669999599456787,
		1419976200: 0.21330000460147858,
		1419976800: 0.17329999804496765,
	}

	for _, dp := range dps {
		if dp.Value != expected[dp.Timestamp] {
			t.Errorf("Expected %v, got %v for %v", expected[dp.Timestamp], dp.Value, dp.Timestamp)
		}
	}
	if len(dps) != len(expected) {
		t.Errorf("Expected count is %v, got %v", len(expected), len(dps))
	}
}

func TestReadAndCompareWithFractionOfHour(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	client := New("db1.kronos.d,db2.kronos.d,db3.kronos.d")

	// Expected:
	// http://tsdb.kronos.d:4242/api/query/?start=2014/12/31-02:15:00&end=2014/12/31-04:40:00&m=mimmax:php.requests.realty.ngs.ru.rps{script=General.php,status=200,region=54}&json
	var start int32 = 1419966000 + 900
	var end int32 = 1419976800 - 1200

	log.Printf("%v -> %v", start, end)

	tags := opentsdb.Tags{
		"script": "General.php",
		"region": "54",
		"status": "200",
	}

	dps, err := client.GetDatapoints(start, end, "php.requests.realty.ngs.ru.rps", tags)

	if err != nil {
		log.Fatalf("Failed to get metrics: %v", err)
	}

	expected := map[int64]float32{
		1419967200: 0.33169999718666077,
		1419967800: 0.2800000011920929,
		1419968400: 0.2732999920845032,
		1419969000: 0.26170000433921814,
		1419969600: 0.30169999599456787,
		1419970200: 0.23669999837875366,
		1419970800: 0.20499999821186066,
		1419971400: 0.29499998688697815,
		1419972000: 0.1899999976158142,
		1419972600: 0.22169999778270721,
		1419973200: 0.21170000731945038,
		1419973800: 0.19169999659061432,
		1419974400: 0.17669999599456787,
		1419975000: 0.19329999387264252,
		1419975600: 0.17669999599456787,
	}

	for _, dp := range dps {
		if dp.Value != expected[dp.Timestamp] {
			t.Errorf("Expected %v, got %v for %v", expected[dp.Timestamp], dp.Value, dp.Timestamp)
		}
	}
	if len(dps) != len(expected) {
		t.Errorf("Expected count is %v, got %v", len(expected), len(dps))
	}
}
