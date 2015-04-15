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
	outs                map[string]chan<- []byte
	currentDBCollection string
	buf                 [db.MaxBSONSize]byte
}

func (dmx *Demultiplexer) run() error {
	parse := NewArchiveParser(dmx.in, dmx)
	return parse.Run()
}

func (dmx *Demultiplexer) HandleOutOfBandBSON(buf []byte) error {
	colHeader := CollectionHeader{}
	err := bson.Unmarshal(buf, &colHeader)
	if err != nil {
		return fmt.Errorf("%v; out of band bson doesn't unmarshal as a collection header: (%v)", errorSuffix, err)
	}
	if colHeader.Database == "" {
		return fmt.Errorf("%v; collection header is missing a Database", errorSuffix)
	}
	if colHeader.Collection == "" {
		return fmt.Errorf("%v; collection header is missing a Collection", errorSuffix)
	}
	dmx.currentDBCollection = colHeader.Database + "/" + colHeader.Collection
	if _, ok := dmx.outs[dmx.currentDBCollection]; !ok {
		return fmt.Errorf("%v; collection header specifies db/collection that shouldn't be in the archive, or is already closed")
	}
	if colHeader.EOF {
		close(dmx.outs[dmx.currentDBCollection])
		delete(dmx.outs, dmx.currentDBCollection)
	}
	return nil
}

func (dmx *Demultiplexer) End() error {
	//TODO, check that the outs are all closed here and error if they are not
	return nil
}

func (dmx *Demultiplexer) DispatchBSON(buf []byte) error {
	if dmx.currentDBCollection == "" {
		return fmt.Errorf("%v; collection data without a collection header")
	}
	dmx.outs[dmx.currentDBCollection] <- buf
	return nil
}

type DemuxOut struct {
	out <-chan []byte
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
	return nil
}
func (dmxOut *DemuxOut) Write([]byte) (int, error) {
	return 0, nil
}
