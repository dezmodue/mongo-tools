package archive

import (
	"bytes"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/intents"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/mgo.v2/bson"
	"hash"
	"hash/crc32"
	"testing"
	"time"
)

var testIntents = []*intents.Intent{
	&intents.Intent{
		DB:       "foo",
		C:        "bar",
		BSONPath: "foo.bar",
	},
	&intents.Intent{
		DB:       "ding",
		C:        "bats",
		BSONPath: "ding.bats",
	},
	&intents.Intent{
		DB:       "flim",
		C:        "flam.fooey",
		BSONPath: "flim.flam.fooey",
	},
	&intents.Intent{
		DB:       "crow",
		C:        "bar",
		BSONPath: "crow.bar",
	},
}

type testDoc struct {
	Bar int
	Baz string
}

func TestBasicMux(t *testing.T) {
	var err error

	Convey("10000 docs in each of five collections multiplexed and demultiplexed", t, func() {
		buf := &bytes.Buffer{}

		mux := &Multiplexer{Out: buf}
		muxIns := map[string]*MuxIn{}

		inChecksum := map[string]hash.Hash{}
		inLength := map[string]int{}
		outChecksum := map[string]hash.Hash{}
		outLength := map[string]int{}

		for _, dbc := range testIntents {
			inChecksum[dbc.Namespace()] = crc32.NewIEEE()
			muxIns[dbc.Namespace()] = &MuxIn{Intent: dbc, Mux: mux}
			err = muxIns[dbc.Namespace()].Open()
		}
		for index, dbc := range testIntents {
			closeDbc := dbc
			go func() {
				defer muxIns[closeDbc.Namespace()].Close()
				staticBSONBuf := make([]byte, db.MaxBSONSize)
				for i := 0; i < 10000; i++ {

					bsonMarshal, _ := bson.Marshal(testDoc{Bar: index * i, Baz: closeDbc.Namespace()})
					bsonBuf := staticBSONBuf[:len(bsonMarshal)]
					copy(bsonBuf, bsonMarshal)
					muxIns[closeDbc.Namespace()].Write(bsonBuf)
					inChecksum[closeDbc.Namespace()].Write(bsonBuf)
					inLength[closeDbc.Namespace()] += len(bsonBuf)
				}
			}()
		}
		mux.Run()

		demux := &Demultiplexer{in: buf}
		demuxOuts := map[string]*DemuxOut{}

		for _, dbc := range testIntents {
			outChecksum[dbc.Namespace()] = crc32.NewIEEE()
			demuxOuts[dbc.Namespace()] = &DemuxOut{Intent: dbc, Demux: demux}
			demuxOuts[dbc.Namespace()].Open()
		}

		for _, dbc := range testIntents {
			closeDbc := dbc
			go func() {
				bs := make([]byte, db.MaxBSONSize)
				var readErr error
				//var length int
				var i int
				for {
					i++
					var length int
					length, readErr = demuxOuts[closeDbc.Namespace()].Read(bs)
					//		fmt.Fprintf(os.Stderr, "%v\n", bs[:length])
					if readErr != nil {
						break
					}
					outChecksum[closeDbc.Namespace()].Write(bs[:length])
					outLength[closeDbc.Namespace()] += len(bs[:length])
				}
			}()
		}
		demux.Run()
		time.Sleep(time.Second)
		for _, dbc := range testIntents {
			So(inLength[dbc.Namespace()], ShouldEqual, outLength[dbc.Namespace()])
			So(inChecksum[dbc.Namespace()].Sum([]byte{}), ShouldResemble, outChecksum[dbc.Namespace()].Sum([]byte{}))
		}
	})
	return
}
