package archive

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
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
	selectCases             []reflect.SelectCase
	currentDBCollection     string
	buf                     [db.MaxBSONSize]byte
}

func (mux *Multiplexer) Run() (err error) {
	var delimiterBytes []byte = []byte{0xFF, 0xFF, 0xFF, 0xFF} // TODO, rectify this with delimiter
	for {
		selectCases, selectCasesDBCollection := mux.getSelectCases()
		if len(selectCases) == 0 {
			// you must start writers before you start the mux, otherwise the mux will just finish thinking there is no more work to do
			return nil
		}
		index, value, ok := reflect.Select(selectCases)
		if !ok {
			_, err = mux.out.Write(delimiterBytes)
			if err != nil {
				return err
			}
			dbCollectionParts := strings.SplitN(mux.selectCasesDBCollection[index], ".", 2)
			eofHeader, err := bson.Marshal(CollectionHeader{Database: dbCollectionParts[0], Collection: dbCollectionParts[1], EOF: true})
			if err != nil {
				return err
			}
			_, err = mux.out.Write(eofHeader)
			if err != nil {
				return err
			}
			mux.currentDBCollection = ""
		} else {
			if selectCasesDBCollection[index] != mux.currentDBCollection {
				_, err = mux.out.Write(delimiterBytes)
				if err != nil {
					return err
				}
				dbCollectionParts := strings.SplitN(selectCasesDBCollection[index], ".", 2)
				header, err := bson.Marshal(CollectionHeader{Database: dbCollectionParts[0], Collection: dbCollectionParts[1]})
				if err != nil {
					return err
				}
				_, err = mux.out.Write(header)
				if err != nil {
					return err
				}
				mux.currentDBCollection = selectCasesDBCollection[index]
			}
			bsonBytes, ok := value.Interface().([]byte)
			if !ok {
				return fmt.Errorf("Multiplexer received a value that wasn't a []byte")
			}
			_, err = mux.out.Write(bsonBytes)
			if err != nil {
				return err
			}
		}
	}
}

func (mux *Multiplexer) getSelectCases() ([]reflect.SelectCase, []string) {
	mux.selectCasesLock.Lock()
	defer mux.selectCasesLock.Unlock()
	return mux.selectCases, mux.selectCasesDBCollection
}

func (mux *Multiplexer) close(dbCollection string) {
	mux.selectCasesLock.Lock()
	defer mux.selectCasesLock.Unlock()
	for index, dbc := range mux.selectCasesDBCollection {
		if dbc == dbCollection {
			// create brand new slices to avoid clobbering any acquired via getSelectCases()
			selectCasesDBCollection := make([]string, 0, len(mux.selectCasesDBCollection)-1)
			selectCasesDBCollection = append(selectCasesDBCollection, mux.selectCasesDBCollection[:index]...)
			selectCasesDBCollection = append(selectCasesDBCollection, mux.selectCasesDBCollection[index+1:]...)
			mux.selectCasesDBCollection = selectCasesDBCollection

			selectCases := make([]reflect.SelectCase, 0, len(mux.selectCases)-1)
			selectCases = append(selectCases, mux.selectCases[:index]...)
			selectCases = append(selectCases, mux.selectCases[index+1:]...)
			mux.selectCases = selectCases
		}
	}
}

func (mux *Multiplexer) open(dbCollection string) chan []byte {
	mux.selectCasesLock.Lock()
	defer mux.selectCasesLock.Unlock()
	in := make(chan []byte)

	// create brand new slices to avoid clobbering any acquired via getSelectCases()
	selectCasesDBCollection := make([]string, 0, len(mux.selectCasesDBCollection)+1)
	copy(selectCasesDBCollection, mux.selectCasesDBCollection)
	selectCasesDBCollection = append(selectCasesDBCollection, dbCollection)
	mux.selectCasesDBCollection = selectCasesDBCollection

	mux.selectCasesDBCollection = selectCasesDBCollection
	selectCases := make([]reflect.SelectCase, 0, len(mux.selectCases)+1)
	copy(selectCases, mux.selectCases)
	selectCases = append(selectCases, reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(in), reflect.Value{}})
	mux.selectCases = selectCases

	return in
}

// MuxIn's live in the intents, and potentially owned by different threads than
// the thread owning the Multiplexer
type MuxIn struct {
	in           chan<- []byte
	dbcollection string
	mux          *Multiplexer
}

func (mxIn *MuxIn) Read([]byte) (int, error) {
	return 0, nil
}

func (mxIn *MuxIn) Close() error {
	close(mxIn.in)
	return nil
}
func (mxIn *MuxIn) Open() error {
	mxIn.in = mxIn.mux.open(mxIn.dbcollection)
	return nil
}
func (mxIn *MuxIn) Write(buf []byte) (int, error) {
	mxIn.in <- buf
	return len(buf), nil
}
