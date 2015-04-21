package archive

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	"gopkg.in/mgo.v2/bson"
	"io"
)

var errorSuffix string = "Dump archive format error"

type Demultiplexer struct {
	in                  io.Reader
	outs                map[string]*DemuxOut
	currentDBCollection string
	buf                 [db.MaxBSONSize]byte
}

func (dmx *Demultiplexer) Run() error {
	parse := Parser{In: dmx.in}
	return parse.ReadAllBlocks(dmx)
}

func (dmx *Demultiplexer) HeaderBSON(buf []byte) error {
	colHeader := CollectionHeader{}
	err := bson.Unmarshal(buf, &colHeader)
	if err != nil {
		return fmt.Errorf("%v; header bson doesn't unmarshal as a collection header: (%v)", errorSuffix, err)
	}
	if colHeader.Database == "" {
		return fmt.Errorf("%v; collection header is missing a Database", errorSuffix)
	}
	if colHeader.Collection == "" {
		return fmt.Errorf("%v; collection header is missing a Collection", errorSuffix)
	}
	dmx.currentDBCollection = colHeader.Database + "." + colHeader.Collection
	if _, ok := dmx.outs[dmx.currentDBCollection]; !ok {
		return fmt.Errorf("%v; collection, %v, header specifies db/collection that shouldn't be in the archive, or is already closed",
			errorSuffix, dmx.currentDBCollection, dmx.outs)
	}
	if colHeader.EOF {
		dmx.outs[dmx.currentDBCollection].Close()
		delete(dmx.outs, dmx.currentDBCollection)
		dmx.currentDBCollection = ""
	}
	return nil
}

func (dmx *Demultiplexer) End() error {
	//TODO, check that the outs are all closed here and error if they are not
	if len(dmx.outs) != 0 {
		return fmt.Errorf("%v; archive finished but contained files were unfinished", errorSuffix)
	}
	return nil
}

func (dmx *Demultiplexer) BodyBSON(buf []byte) error {
	if dmx.currentDBCollection == "" {
		return fmt.Errorf("%v; collection data without a collection header", errorSuffix)
	}
	dmx.outs[dmx.currentDBCollection].readLen <- 0
	readBuf := <-dmx.outs[dmx.currentDBCollection].readBuf
	copy(readBuf, buf)
	dmx.outs[dmx.currentDBCollection].readLen <- len(buf)
	return nil
}

type DemuxOut struct {
	readLen      chan int
	readBuf      chan []byte
	dbCollection string
	demux        *Demultiplexer
}

func (dmxOut *DemuxOut) Read(p []byte) (int, error) {
	// Since we're the "reader" here, not the "writer" we need to start with a read, in case the chan is closed
	_, ok := <-dmxOut.readLen
	if !ok {
		return 0, io.EOF
	}
	dmxOut.readBuf <- p
	length := <-dmxOut.readLen
	return length, nil
}

func (dmxOut *DemuxOut) Close() error {
	close(dmxOut.readLen)
	close(dmxOut.readBuf)
	return nil
}
func (dmxOut *DemuxOut) Open() error {
	if dmxOut.demux.outs == nil {
		dmxOut.demux.outs = make(map[string]*DemuxOut)
	}
	dmxOut.readLen = make(chan int)
	dmxOut.readBuf = make(chan []byte)
	// TODO, figure out weather we need to lock around accessing outs
	dmxOut.demux.outs[dmxOut.dbCollection] = dmxOut
	return nil
}
func (dmxOut *DemuxOut) Write([]byte) (int, error) {
	return 0, nil
}
