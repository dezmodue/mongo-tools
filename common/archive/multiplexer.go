package archive

import (
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"io"
	"reflect"
	"strings"
	"sync"
)

type Multiplexer struct {
	out                     io.Writer
	selectCasesLock         sync.Mutex
	selectCasesDBCollection []string
	ins                     []*MuxIn
	selectCases             []reflect.SelectCase
	currentDBCollection     string
}

func (mux *Multiplexer) Run() (err error) {
	for {
		selectCases, ins := mux.getSelectCases()
		if len(selectCases) == 0 {
			// you must start writers before you start the mux, otherwise the mux will just finish thinking there is no more work to do
			return nil
		}
		index, value, selectOk := reflect.Select(selectCases)
		bsonBytes, ok := value.Interface().([]byte)
		if !ok {
			return fmt.Errorf("Multiplexer received a value that wasn't a []byte")
		}
		if !selectOk {
			if mux.currentDBCollection != "" {
				_, err = mux.out.Write(terminatorBytes)
				if err != nil {
					return err
				}
			}
			dbCollectionParts := strings.SplitN(mux.ins[index].dbCollection, ".", 2)
			eofHeader, err := bson.Marshal(CollectionHeader{Database: dbCollectionParts[0], Collection: dbCollectionParts[1], EOF: true})
			if err != nil {
				return err
			}
			_, err = mux.out.Write(eofHeader)
			if err != nil {
				return err
			}
			_, err = mux.out.Write(terminatorBytes)
			if err != nil {
				return err
			}
			mux.currentDBCollection = ""
			mux.close(index)
		} else {
			if ins[index].dbCollection != mux.currentDBCollection {
				// Handle the change of which DB/Collection we're writing docs for
				if mux.currentDBCollection != "" {
					_, err = mux.out.Write(terminatorBytes)
					if err != nil {
						return err
					}
				}
				dbCollectionParts := strings.SplitN(ins[index].dbCollection, ".", 2)
				header, err := bson.Marshal(CollectionHeader{Database: dbCollectionParts[0], Collection: dbCollectionParts[1]})
				if err != nil {
					return err
				}
				_, err = mux.out.Write(header)
				if err != nil {
					return err
				}
			}
			mux.currentDBCollection = ins[index].dbCollection
			length, err := mux.out.Write(bsonBytes)
			if err != nil {
				return err
			}
			ins[index].writeLenCh <- length
		}
	}
}

func (mux *Multiplexer) getSelectCases() ([]reflect.SelectCase, []*MuxIn) {
	mux.selectCasesLock.Lock()
	defer mux.selectCasesLock.Unlock()
	return mux.selectCases, mux.ins
}

func (mux *Multiplexer) close(index int) {
	mux.selectCasesLock.Lock()
	defer mux.selectCasesLock.Unlock()
	// create brand new slices to avoid clobbering any acquired via getSelectCases()
	ins := make([]*MuxIn, 0, len(mux.ins)-1)
	ins = append(ins, mux.ins[:index]...)
	ins = append(ins, mux.ins[index+1:]...)
	mux.ins = ins

	selectCases := make([]reflect.SelectCase, 0, len(mux.selectCases)-1)
	selectCases = append(selectCases, mux.selectCases[:index]...)
	selectCases = append(selectCases, mux.selectCases[index+1:]...)
	mux.selectCases = selectCases
}

func (mux *Multiplexer) open(min *MuxIn) {
	mux.selectCasesLock.Lock()
	defer mux.selectCasesLock.Unlock()
	writeCh := make(chan []byte)
	min.writeCh = writeCh
	min.writeLenCh = make(chan int)
	// create brand new slices to avoid clobbering any acquired via getSelectCases()
	ins := make([]*MuxIn, 0, len(mux.ins)+1)
	ins = append(ins, mux.ins...)
	mux.ins = append(ins, min)

	selectCases := make([]reflect.SelectCase, 0, len(mux.selectCases)+1)
	selectCases = append(selectCases, mux.selectCases...)
	mux.selectCases = append(selectCases, reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(writeCh), reflect.Value{}})
}

// MuxIn's live in the intents, and are potentially owned by different threads than
// the thread owning the Multiplexer
type MuxIn struct {
	writeCh      chan<- []byte
	writeLenCh   chan int
	dbCollection string
	mux          *Multiplexer
}

func (mxIn *MuxIn) Read([]byte) (int, error) {
	return 0, nil
}

func (mxIn *MuxIn) Close() error {
	// the mux side of this gets closed in the mux when it gets an eof on the read
	close(mxIn.writeCh)
	close(mxIn.writeLenCh)
	return nil
}
func (mxIn *MuxIn) Open() error {
	mxIn.mux.open(mxIn)
	return nil
}
func (mxIn *MuxIn) Write(buf []byte) (int, error) {
	mxIn.writeCh <- buf
	length := <-mxIn.writeLenCh
	return length, nil
}
