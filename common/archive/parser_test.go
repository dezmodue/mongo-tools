package archive

import (
	"bytes"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

type testConsumer struct {
	oobd []string // out of band data
	ibd  []string // in band data
	eof  bool
}

func (tc *testConsumer) HandleOutOfBandBSON(b []byte) error {
	ss := strStruct{}
	err := bson.Unmarshal(b, &ss)
	tc.oobd = append(tc.oobd, ss.Str)
	return err
}

func (tc *testConsumer) DispatchBSON(b []byte) error {
	ss := strStruct{}
	err := bson.Unmarshal(b, &ss)
	tc.ibd = append(tc.ibd, ss.Str)
	return err
}

func (tc *testConsumer) End() (err error) {
	if tc.eof {
		err = fmt.Errorf("double end")
	}
	tc.eof = true
	return err
}

type strStruct struct {
	Str string
}

func TestParsing(t *testing.T) {

	Convey("with a parser with a simple parse consumer", t, func() {
		tc := &testConsumer{}
		parser := Parser{consumer: tc}
		Convey("with well formed in and out of band data", func() {
			buf := bytes.Buffer{}
			buf.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
			b, _ := bson.Marshal(strStruct{"out of band"})
			buf.Write(b)
			b, _ = bson.Marshal(strStruct{"in band"})
			buf.Write(b)
			parser.in = &buf
			err := parser.Run()
			So(err, ShouldBeNil)
			So(tc.eof, ShouldBeTrue)
			So(tc.oobd[0], ShouldEqual, "out of band")
			So(tc.ibd[0], ShouldEqual, "in band")
		})
		Convey("with in band data before the out of band data", func() {
			buf := bytes.Buffer{}
			b, _ := bson.Marshal(strStruct{"in band"})
			buf.Write(b)
			buf.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
			b, _ = bson.Marshal(strStruct{"out of band"})
			buf.Write(b)
			parser.in = &buf
			err := parser.Run()
			So(err, ShouldBeNil)
			So(tc.eof, ShouldBeTrue)
			So(tc.oobd[0], ShouldEqual, "out of band")
			So(tc.ibd[0], ShouldEqual, "in band")
		})
		Convey("with an incorrect delimiter", func() {
			buf := bytes.Buffer{}
			buf.Write([]byte{0xFF, 0xFF, 0xFF, 0xFE})
			b, _ := bson.Marshal(strStruct{"out of band"})
			buf.Write(b)
			b, _ = bson.Marshal(strStruct{"in band"})
			buf.Write(b)
			parser.in = &buf
			err := parser.Run()
			So(err, ShouldNotBeNil)
		})
		Convey("with an error comming from End", func() {
			tc.eof = true
			buf := bytes.Buffer{}
			buf.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
			b, _ := bson.Marshal(strStruct{"out of band"})
			buf.Write(b)
			b, _ = bson.Marshal(strStruct{"in band"})
			buf.Write(b)
			parser.in = &buf
			err := parser.Run()
			So(err, ShouldNotBeNil)
			So(tc.eof, ShouldBeTrue)
			So(tc.oobd[0], ShouldEqual, "out of band")
			So(tc.ibd[0], ShouldEqual, "in band")
		})
		Convey("with an early EOF", func() {
			buf := bytes.Buffer{}
			buf.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
			b, _ := bson.Marshal(strStruct{"out of band"})
			buf.Write(b)
			b, _ = bson.Marshal(strStruct{"in band"})
			buf.Write(b[:len(b)-1])
			parser.in = &buf
			err := parser.Run()
			So(err, ShouldNotBeNil)
			So(tc.eof, ShouldBeFalse)
			So(tc.oobd[0], ShouldEqual, "out of band")
			So(tc.ibd, ShouldBeNil)
		})
		Convey("with an bson without a null terminator", func() {
			buf := bytes.Buffer{}
			buf.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
			b, _ := bson.Marshal(strStruct{"out of band"})
			buf.Write(b)
			b, _ = bson.Marshal(strStruct{"in band"})
			buf.Write(b[:len(b)-1])
			buf.Write([]byte{0x01})
			parser.in = &buf
			err := parser.Run()
			So(err, ShouldNotBeNil)
			So(tc.eof, ShouldBeFalse)
			So(tc.oobd[0], ShouldEqual, "out of band")
			So(tc.ibd, ShouldBeNil)
		})
	})
	return
}
