package mongodump

import (
	"bufio"
	"fmt"
	"github.com/dezmodue/mongo-tools/common/bsonutil"
	"github.com/dezmodue/mongo-tools/common/db"
	"github.com/dezmodue/mongo-tools/common/intents"
	"github.com/dezmodue/mongo-tools/common/json"
	"github.com/dezmodue/mongo-tools/common/log"
	"gopkg.in/mgo.v2/bson"
)

// Metadata holds information about a collection's options and indexes.
type Metadata struct {
	Options interface{}   `json:"options,omitempty"`
	Indexes []interface{} `json:"indexes"`
}

// IndexDocumentFromDB is used internally to preserve key ordering.
type IndexDocumentFromDB struct {
	Options bson.M `bson:",inline"`
	Key     bson.D `bson:"key"`
}

// dumpMetadata gets the metadata for a collection and writes it
// in readable JSON format.
func (dump *MongoDump) dumpMetadata(intent *intents.Intent) error {
	var err error
	err = intent.MetadataFile.Open()
	if err != nil {
		return err
	}
	defer intent.MetadataFile.Close()
	// make a buffered writer for nicer disk i/o
	w := bufio.NewWriter(intent.MetadataFile)

	nsID := fmt.Sprintf("%v.%v", intent.DB, intent.C)
	meta := Metadata{
		// We have to initialize Indexes to an empty slice, not nil, so that an empty
		// array is marshalled into json instead of null. That is, {indexes:[]} is okay
		// but {indexes:null} will cause assertions in our legacy C++ mongotools
		Indexes: []interface{}{},
	}

	// The collection options were already gathered while building the list of intents.
	// We convert them to JSON so that they can be written to the metadata json file as text.
	if intent.Options != nil {
		if meta.Options, err = bsonutil.ConvertBSONValueToJSON(*intent.Options); err != nil {
			return fmt.Errorf("error converting collection options to JSON: %v", err)
		}
	} else {
		meta.Options = nil
	}

	// Second, we read the collection's index information by either calling
	// listIndexes (pre-2.7 systems) or querying system.indexes.
	// We keep a running list of all the indexes
	// for the current collection as we iterate over the cursor, and include
	// that list as the "indexes" field of the metadata document.
	log.Logf(log.DebugHigh, "\treading indexes for `%v`", nsID)

	session, err := dump.sessionProvider.GetSession()
	if err != nil {
		return err
	}
	defer session.Close()

	// get the indexes
	indexesIter, err := db.GetIndexes(session.DB(intent.DB).C(intent.C))
	if err != nil {
		return err
	}

	indexOpts := &bson.D{}
	for indexesIter.Next(indexOpts) {
		convertedIndex, err := bsonutil.ConvertBSONValueToJSON(*indexOpts)
		if err != nil {
			return fmt.Errorf("error converting index (%#v): %v", convertedIndex, err)
		}
		meta.Indexes = append(meta.Indexes, convertedIndex)
	}

	if err := indexesIter.Err(); err != nil {
		return fmt.Errorf("error getting indexes for collection `%v`: %v", nsID, err)
	}

	// Finally, we send the results to the writer as JSON bytes
	jsonBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("error marshalling metadata json for collection `%v`: %v", nsID, err)
	}
	_, err = w.Write(jsonBytes)
	if err != nil {
		return fmt.Errorf("error writing metadata for collection `%v` to disk: %v", nsID, err)
	}
	err = w.Flush()
	if err != nil {
		return fmt.Errorf("error writing metadata for collection `%v` to disk: %v", nsID, err)
	}
	return nil
}
