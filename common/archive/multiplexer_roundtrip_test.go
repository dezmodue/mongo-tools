package archive

import (
	"bytes"
	"github.com/mongodb/mongo-tools/common/db"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/mgo.v2/bson"
	"hash"
	"hash/crc32"
	"testing"
	"time"
)

var dbCollections = []string{
	"foo.bar",
	"ding.bats",
	"ding.dong",
	"flim.flam.fooey",
	"crow.bar",
}

type foo struct {
	Bar int
	Baz string
}

func TestBasicMux(t *testing.T) {
	var err error

	Convey("10000 docs in each of five collections multiplexed and demultiplexed", t, func() {
		buf := &bytes.Buffer{}

		mux := &Multiplexer{out: buf}
		muxIns := map[string]*MuxIn{}

		inChecksum := map[string]hash.Hash{}
		inLength := map[string]int{}
		outChecksum := map[string]hash.Hash{}
		outLength := map[string]int{}

		for _, dbc := range dbCollections {
			inChecksum[dbc] = crc32.NewIEEE()
			muxIns[dbc] = &MuxIn{dbCollection: dbc, mux: mux}
			err = muxIns[dbc].Open()
		}
		for index, dbc := range dbCollections {
			closeDbc := dbc
			go func() {
				for i := 0; i < 10000; i++ {
					bson, _ := bson.Marshal(foo{Bar: index * i, Baz: closeDbc})
					muxIns[closeDbc].Write(bson)
					inChecksum[closeDbc].Write(bson)
					//					fmt.Fprintf(os.Stderr, "%v\n", bson)
					inLength[closeDbc] += len(bson)
				}
				muxIns[closeDbc].Close()
			}()
		}
		mux.Run()

		demux := &Demultiplexer{in: buf}
		demuxOuts := map[string]*DemuxOut{}

		for _, dbc := range dbCollections {
			outChecksum[dbc] = crc32.NewIEEE()
			demuxOuts[dbc] = &DemuxOut{dbCollection: dbc, demux: demux}
			demuxOuts[dbc].Open()
		}

		for _, dbc := range dbCollections {
			closeDbc := dbc
			go func() {
				bs := make([]byte, db.MaxBSONSize)
				var readErr error
				//var length int
				var i int
				for {
					i++
					var length int
					length, readErr = demuxOuts[closeDbc].Read(bs)
					//		fmt.Fprintf(os.Stderr, "%v\n", bs[:length])
					if readErr != nil {
						break
					}
					outChecksum[closeDbc].Write(bs[:length])
					outLength[closeDbc] += len(bs[:length])
				}
			}()
		}
		demux.Run()
		time.Sleep(time.Second)
		for _, dbc := range dbCollections {
			So(inLength[dbc], ShouldEqual, outLength[dbc])
			So(string(inChecksum[dbc].Sum([]byte{})), ShouldEqual, string(outChecksum[dbc].Sum([]byte{})))
		}
	})
	return
}
