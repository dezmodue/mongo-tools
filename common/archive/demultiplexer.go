package archive

import (
	"encoding/bson"
	"io"
)

var errorSuffix string = "Dump archive format error"

type collectionHeader struct {
	Database   string `bson:"db"`
	Collection string `bson:"collection"`
	EOF        bool   `bson:"EOF",omitempty`
}

type Demultiplexer struct {
	in                  io.Reader
	outs                map[string]<-chan []byte
	currentDBCollection string
	buf                 [db.MaxBSONSize]byte
}

func (dmx *Demultiplexer) run() error {
	parse := NewArchiveParser(dmx.in, dmx)
	return parse.run()
}

func (dmx *Demultiplexer) HandleOutOfBandBSON(buf []byte) error {
	colHeader := collectionHeader{}
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
	if dmx.EOF {
		close(dmx.outs[dmx.currentDBCollection])
		delete(dmx.outs, dmx.currentDBCollection)
	}
	return nil
}

func (dmx *Demultiplexer) DispatchBSON(buf []byte) error {
	if currentDBCollection == "" {
		return fmt.Errorf("%v; collection data without a collection header")
	}
	outs[currentDBCollection] <- buf
	return nil
}

type DemuxOut struct {
	in chan<- []byte
}

func (dmxOut *DemuxOut) Read(p []byte) (int, error) {
	p, ok := <-dmxOut.in
	if !ok {
		return 0, oi.EOF
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
	return nil
}
