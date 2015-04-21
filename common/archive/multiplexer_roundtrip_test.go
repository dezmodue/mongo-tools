package archive

import (
	"bytes"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/mgo.v2/bson"
	"os"
	"testing"
)

var dbCollections = []string{
	"foo.bar",
	"ding.bats",
	//	"ding.dong",
	//	"flim.flam.fooey",
	//	"crow.bar",
}

type foo struct {
	Bar int
	Baz string
}

func TestBasicMux(t *testing.T) {
	var err error

	Convey("basic multiplexing", t, func() {
		buf := &bytes.Buffer{}

		mux := &Multiplexer{out: buf}
		muxIns := map[string]*MuxIn{}

		for _, dbc := range dbCollections {
			muxIns[dbc] = &MuxIn{dbCollection: dbc, mux: mux}
			err = muxIns[dbc].Open()
		}
		for index, dbc := range dbCollections {
			closeDbc := dbc
			go func() {
				bson, _ := bson.Marshal(foo{Bar: index, Baz: closeDbc})
				muxIns[closeDbc].Write(bson)
				muxIns[closeDbc].Close()
			}()
		}
		mux.Run()
		Printf("out: %v %v\n", len(buf.Bytes()), buf.Bytes())

		demux := &Demultiplexer{in: buf}
		demuxOuts := map[string]*DemuxOut{}

		for _, dbc := range dbCollections {
			demuxOuts[dbc] = &DemuxOut{dbCollection: dbc, demux: demux}
			demuxOuts[dbc].Open()
		}

		for _, dbc := range dbCollections {
			closeDbc := dbc
			go func() {
				fmt.Fprintf(os.Stderr, "reader starting %v\n", closeDbc)
				bson := []byte{}
				err, length := demuxOuts[closeDbc].Read(bson)
				fmt.Fprintf(os.Stderr, "read %v %v\n", err, length)
				fmt.Fprintf(os.Stderr, "reader finished %v\n", closeDbc)
			}()
		}
		demux.Run()
	})
	return
}
