package archive

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	"gopkg.in/mgo.v2/bson"
	"io"
	//	"os"
)

var errorSuffix string = "Dump archive format error"

type Demultiplexer struct {
	in                  io.Reader
	outs                map[string]chan<- []byte
	currentDBCollection string
	buf                 [db.MaxBSONSize]byte
}

func (dmx *Demultiplexer) Run() error {
	parse := Parser{in: dmx.in}
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
		return fmt.Errorf("%v; collection, %v, header specifies db/collection that shouldn't be in the archive, or is already closed", errorSuffix, dmx.currentDBCollection, dmx.outs)
	}
	if colHeader.EOF {
		close(dmx.outs[dmx.currentDBCollection])
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
	dmx.outs[dmx.currentDBCollection] <- buf
	return nil
}

type DemuxOut struct {
	out          <-chan []byte
	dbCollection string
	demux        *Demultiplexer
}

func (dmxOut *DemuxOut) Read(p []byte) (int, error) {
	p, ok := <-dmxOut.out
	if !ok {
		return 0, io.EOF
	}
	return len(p), nil
}

func (dmxOut *DemuxOut) Close() error {
	return nil
}
func (dmxOut *DemuxOut) Open() error {
	out := make(chan []byte)
	if dmxOut.demux.outs == nil {
		dmxOut.demux.outs = make(map[string]chan<- []byte)
	}
	// either we need to lock around all uses of the outs map, or make sure that all uses of the map happen in one thred
	dmxOut.demux.outs[dmxOut.dbCollection] = out
	dmxOut.out = out
	return nil
}
func (dmxOut *DemuxOut) Write([]byte) (int, error) {
	return 0, nil
}
