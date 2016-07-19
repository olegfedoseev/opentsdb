// +build integration
package reader

import (
	//	"fmt"
	"log"
	"testing"
	"time"
)

// go test -tags=integration

func TestReadAndCompare(t *testing.T) {
	client := New("db1.kronos.d,db2.kronos.d,db3.kronos.d")

	// Expected:
	// http://tsdb.kronos.d:4242/api/query/?start=2016/01/21-14:00:00&end=2016/01/21-15:00:00&m=mimmax:php.requests.api.zp.ru.rps{script=Job_Api_Categories__find}&json
	start, _ := time.Parse("2006/01/02-15:04:05", "2016/01/21-14:00:00")
	end, _ := time.Parse("2006/01/02-15:04:05", "2016/01/21-15:00:00")

	dps, err := client.GetDatapoints(start, end, "php.requests.api.zp.ru.rps")

	if err != nil {
		log.Fatalf("Failed to get metrics: %v", err)
	}

	for _, dp := range dps {
		// 21600 - tz shift
		log.Printf("%d: %3.2f (%3.2f)", dp.Timestamp, dp.Value, expected[dp.Timestamp-21600])
	}
	log.Printf("Count: %v/%v", len(dps), len(expected))
}

var (
	// Expected:
	// http://tsdb.kronos.d:4242/api/query/?start=2016/01/21-14:00:00&end=2016/01/21-15:00:00&m=mimmax:php.requests.api.zp.ru.rps{script=Job_Api_Categories__find}&json
	expected = map[int64]float32{
		1453363200: 2.299999952316284,
		1453363210: 2.0999999046325684,
		1453363220: 1.2999999523162842,
		1453363230: 0.699999988079071,
		1453363240: 1.399999976158142,
		1453363250: 2.5999999046325684,
		1453363260: 0.8999999761581421,
		1453363270: 1.600000023841858,
		1453363280: 1.7999999523162842,
		1453363290: 1.600000023841858,
		1453363300: 1,
		1453363310: 1.7000000476837158,
		1453363320: 1.100000023841858,
		1453363330: 1.2000000476837158,
		1453363340: 2.0999999046325684,
		1453363350: 1.2000000476837158,
		1453363360: 1.2999999523162842,
		1453363370: 1.5,
		1453363380: 1.5,
		1453363390: 1.399999976158142,
		1453363400: 1.100000023841858,
		1453363410: 0.8999999761581421,
		1453363420: 0.800000011920929,
		1453363430: 1.899999976158142,
		1453363440: 1.7999999523162842,
		1453363450: 1.2000000476837158,
		1453363460: 1.600000023841858,
		1453363470: 1.5,
		1453363480: 1,
		1453363490: 1.2999999523162842,
		1453363500: 1.100000023841858,
		1453363510: 0.800000011920929,
		1453363520: 0.699999988079071,
		1453363530: 0.699999988079071,
		1453363540: 1,
		1453363550: 1.7000000476837158,
		1453363560: 1.7000000476837158,
		1453363570: 1,
		1453363580: 1,
		1453363590: 0.8999999761581421,
		1453363600: 1.600000023841858,
		1453363610: 0.5,
		1453363620: 1.2000000476837158,
		1453363630: 2.0999999046325684,
		1453363640: 1.600000023841858,
		1453363650: 1,
		1453363660: 1.2000000476837158,
		1453363670: 1.600000023841858,
		1453363680: 0.800000011920929,
		1453363690: 1.7000000476837158,
		1453363700: 1.600000023841858,
		1453363710: 1.399999976158142,
		1453363720: 0.8999999761581421,
		1453363730: 0.699999988079071,
		1453363740: 1.5,
		1453363750: 1.7000000476837158,
		1453363760: 1.399999976158142,
		1453363770: 1.600000023841858,
		1453363780: 1.7000000476837158,
		1453363790: 1.399999976158142,
		1453363800: 2.5,
		1453363810: 1.7000000476837158,
		1453363820: 0.8999999761581421,
		1453363830: 0.699999988079071,
		1453363840: 1.2999999523162842,
		1453363850: 1.2999999523162842,
		1453363860: 2.0999999046325684,
		1453363870: 1,
		1453363880: 0.800000011920929,
		1453363890: 1.100000023841858,
		1453363900: 2.799999952316284,
		1453363910: 0.8999999761581421,
		1453363920: 2.0999999046325684,
		1453363930: 1.399999976158142,
		1453363940: 1.100000023841858,
		1453363950: 1.899999976158142,
		1453363960: 1.7999999523162842,
		1453363970: 2.5,
		1453363980: 1.7000000476837158,
		1453363990: 1.2000000476837158,
		1453364000: 3,
		1453364010: 1.2999999523162842,
		1453364020: 1.2999999523162842,
		1453364030: 1.7999999523162842,
		1453364040: 1.7000000476837158,
		1453364050: 1.600000023841858,
		1453364060: 1.399999976158142,
		1453364070: 2,
		1453364080: 1.7999999523162842,
		1453364090: 1.2000000476837158,
		1453364100: 3.200000047683716,
		1453364110: 2.4000000953674316,
		1453364120: 2.0999999046325684,
		1453364130: 1.7999999523162842,
		1453364140: 1.7000000476837158,
		1453364150: 2.200000047683716,
		1453364160: 1.7999999523162842,
		1453364170: 2.4000000953674316,
		1453364180: 2.299999952316284,
		1453364190: 3.200000047683716,
		1453364200: 2.4000000953674316,
		1453364210: 1.7000000476837158,
		1453364220: 2.200000047683716,
		1453364230: 1.2999999523162842,
		1453364240: 2.4000000953674316,
		1453364250: 1.100000023841858,
		1453364260: 0.800000011920929,
		1453364270: 1.7999999523162842,
		1453364280: 2.4000000953674316,
		1453364290: 0.800000011920929,
		1453364300: 2.0999999046325684,
		1453364310: 2.0999999046325684,
		1453364320: 2.200000047683716,
		1453364330: 1.2000000476837158,
		1453364340: 2,
		1453364350: 1.600000023841858,
		1453364360: 1.7000000476837158,
		1453364370: 1.399999976158142,
		1453364380: 1.899999976158142,
		1453364390: 1.899999976158142,
		1453364400: 1.899999976158142,
		1453364410: 1.899999976158142,
		1453364420: 1.7000000476837158,
		1453364430: 1.399999976158142,
		1453364440: 1.100000023841858,
		1453364450: 1.2999999523162842,
		1453364460: 1.2000000476837158,
		1453364470: 1.7999999523162842,
		1453364480: 2.700000047683716,
		1453364490: 1.5,
		1453364500: 1.399999976158142,
		1453364510: 1.600000023841858,
		1453364520: 1.600000023841858,
		1453364530: 1.899999976158142,
		1453364540: 1.899999976158142,
		1453364550: 1.2999999523162842,
		1453364560: 0.800000011920929,
		1453364570: 2.200000047683716,
		1453364580: 1.2999999523162842,
		1453364590: 1.5,
		1453364600: 1.5,
		1453364610: 0.699999988079071,
		1453364620: 2.299999952316284,
		1453364630: 3.200000047683716,
		1453364640: 2,
		1453364650: 1.399999976158142,
		1453364660: 1.5,
		1453364670: 1.7000000476837158,
		1453364680: 1.2000000476837158,
		1453364690: 1.2000000476837158,
		1453364700: 0.800000011920929,
		1453364710: 0.8999999761581421,
		1453364720: 1.399999976158142,
		1453364730: 1.2999999523162842,
		1453364740: 0.6000000238418579,
		1453364750: 2.299999952316284,
		1453364760: 2,
		1453364770: 1.5,
		1453364780: 1.7000000476837158,
		1453364790: 1.2000000476837158,
		1453364800: 0.6000000238418579,
		1453364810: 1.2000000476837158,
		1453364820: 1.100000023841858,
		1453364830: 1.600000023841858,
		1453364840: 1.100000023841858,
		1453364850: 1,
		1453364860: 1.100000023841858,
		1453364870: 0.8999999761581421,
		1453364880: 1.2000000476837158,
		1453364890: 1.2999999523162842,
		1453364900: 0.8999999761581421,
		1453364910: 0.699999988079071,
		1453364920: 1.5,
		1453364930: 1.2000000476837158,
		1453364940: 0.800000011920929,
		1453364950: 1.100000023841858,
		1453364960: 1.600000023841858,
		1453364970: 1,
		1453364980: 1.2000000476837158,
		1453364990: 1.2999999523162842,
		1453365000: 1,
		1453365010: 1.600000023841858,
		1453365020: 1.399999976158142,
		1453365030: 0.800000011920929,
		1453365040: 1.100000023841858,
		1453365050: 1.2000000476837158,
		1453365060: 1.2000000476837158,
		1453365070: 1.100000023841858,
		1453365080: 1.100000023841858,
		1453365090: 1.2000000476837158,
		1453365100: 1.100000023841858,
		1453365110: 1.600000023841858,
		1453365120: 1.600000023841858,
		1453365130: 1.7000000476837158,
		1453365140: 2.299999952316284,
		1453365150: 1.399999976158142,
		1453365160: 1,
		1453365170: 1.7000000476837158,
		1453365180: 2.299999952316284,
		1453365190: 2,
		1453365200: 1.2000000476837158,
		1453365210: 1.5,
		1453365220: 1.399999976158142,
		1453365230: 1.2999999523162842,
		1453365240: 1.399999976158142,
		1453365250: 1.7999999523162842,
		1453365260: 1,
		1453365270: 1.899999976158142,
		1453365280: 1.5,
		1453365290: 2,
		1453365300: 1.2999999523162842,
		1453365310: 0.6000000238418579,
		1453365320: 1.899999976158142,
		1453365330: 2,
		1453365340: 1.600000023841858,
		1453365350: 0.6000000238418579,
		1453365360: 2,
		1453365370: 1.899999976158142,
		1453365380: 1.600000023841858,
		1453365390: 1.7000000476837158,
		1453365400: 1.399999976158142,
		1453365410: 1.600000023841858,
		1453365420: 1,
		1453365430: 1.5,
		1453365440: 1.2000000476837158,
		1453365450: 1.399999976158142,
		1453365460: 1.899999976158142,
		1453365470: 1.399999976158142,
		1453365480: 0.800000011920929,
		1453365490: 1.2999999523162842,
		1453365500: 1.899999976158142,
		1453365510: 1.2000000476837158,
		1453365520: 1.7000000476837158,
		1453365530: 1.399999976158142,
		1453365540: 1.2000000476837158,
		1453365550: 1.2999999523162842,
		1453365560: 1.2999999523162842,
		1453365570: 0.800000011920929,
		1453365580: 1.899999976158142,
		1453365590: 1.7000000476837158,
		1453365600: 1.2000000476837158,
		1453365610: 1.7000000476837158,
		1453365620: 1.7000000476837158,
		1453365630: 1.2000000476837158,
		1453365640: 1.399999976158142,
		1453365650: 2.0999999046325684,
		1453365660: 1.100000023841858,
		1453365670: 1.5,
		1453365680: 1.399999976158142,
		1453365690: 1,
		1453365700: 1.2999999523162842,
		1453365710: 0.800000011920929,
		1453365720: 2,
		1453365730: 1.2000000476837158,
		1453365740: 2,
		1453365750: 1.2999999523162842,
		1453365760: 0.8999999761581421,
		1453365770: 1.7000000476837158,
		1453365780: 1.600000023841858,
		1453365790: 1.100000023841858,
		1453365800: 2.299999952316284,
		1453365810: 1.100000023841858,
		1453365820: 0.800000011920929,
		1453365830: 1.5,
		1453365840: 1.399999976158142,
		1453365850: 1.899999976158142,
		1453365860: 1.7000000476837158,
		1453365870: 1.399999976158142,
		1453365880: 2.200000047683716,
		1453365890: 0.8999999761581421,
		1453365900: 1.100000023841858,
		1453365910: 0.8999999761581421,
		1453365920: 1.600000023841858,
		1453365930: 1.7999999523162842,
		1453365940: 1.600000023841858,
		1453365950: 1.2999999523162842,
		1453365960: 1.5,
		1453365970: 2.0999999046325684,
		1453365980: 1.5,
		1453365990: 1.7000000476837158,
		1453366000: 1.2999999523162842,
		1453366010: 1,
		1453366020: 1.899999976158142,
		1453366030: 1.7000000476837158,
		1453366040: 1.2999999523162842,
		1453366050: 1.2999999523162842,
		1453366060: 1.2000000476837158,
		1453366070: 1.100000023841858,
		1453366080: 1.399999976158142,
		1453366090: 0.8999999761581421,
		1453366100: 1.2999999523162842,
		1453366110: 2,
		1453366120: 1.2000000476837158,
		1453366130: 1.899999976158142,
		1453366140: 1.5,
		1453366150: 1.2999999523162842,
		1453366160: 1.7999999523162842,
		1453366170: 1.7999999523162842,
		1453366180: 1.600000023841858,
		1453366190: 1.600000023841858,
		1453366200: 1.7999999523162842,
		1453366210: 1.399999976158142,
		1453366220: 0.8999999761581421,
		1453366230: 1.899999976158142,
		1453366240: 2,
		1453366250: 0.800000011920929,
		1453366260: 1.5,
		1453366270: 0.4000000059604645,
		1453366280: 1.7999999523162842,
		1453366290: 1.100000023841858,
		1453366300: 0.8999999761581421,
		1453366310: 1.7000000476837158,
		1453366320: 1.2999999523162842,
		1453366330: 1.5,
		1453366340: 1.7000000476837158,
		1453366350: 1.600000023841858,
		1453366360: 1.100000023841858,
		1453366370: 1.100000023841858,
		1453366380: 1.2000000476837158,
		1453366390: 0.800000011920929,
		1453366430: 1.399999976158142,
		1453366440: 1.7000000476837158,
		1453366450: 1.2999999523162842,
		1453366460: 1.7999999523162842,
		1453366470: 0.8999999761581421,
		1453366480: 1.100000023841858,
		1453366490: 1.2000000476837158,
		1453366500: 1,
		1453366510: 1.5,
		1453366520: 1.7999999523162842,
		1453366530: 0.8999999761581421,
		1453366540: 1,
		1453366550: 1.100000023841858,
		1453366560: 1.7999999523162842,
		1453366570: 1,
		1453366580: 2.200000047683716,
		1453366590: 0.6000000238418579,
		1453366600: 1.100000023841858,
		1453366610: 1,
		1453366620: 1.7000000476837158,
		1453366630: 1.2000000476837158,
		1453366640: 0.8999999761581421,
		1453366650: 1.7000000476837158,
		1453366660: 0.8999999761581421,
		1453366670: 1.2999999523162842,
		1453366680: 0.800000011920929,
		1453366690: 0.8999999761581421,
		1453366700: 1.2999999523162842,
		1453366710: 1,
		1453366720: 1,
		1453366730: 1.5,
		1453366740: 0.8999999761581421,
		1453366750: 1.100000023841858,
		1453366760: 0.699999988079071,
		1453366770: 1.2000000476837158,
		1453366780: 1,
		1453366790: 0.6000000238418579,
		1453366800: 1.899999976158142,
	}
)
