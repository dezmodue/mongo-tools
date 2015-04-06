package archive

import (
	"io"
)

type Demultiplexer struct {
	in   io.Reader
	outs map[string]<-chan []byte
	buf  [db.MaxBSONSize]byte
}

func (dmx *Demultiplexer) demultiplex error {

		return parse.run()
}

func (dmx *Demultiplexer) HandleOutOfBandBSON(parse.buf, parse.length) {

}

func (dmx *Demultiplexer) DispatchBSON(parse.buf, parse.length) {

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
