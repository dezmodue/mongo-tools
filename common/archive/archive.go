package archive

type CollectionHeader struct {
	Database   string `bson:"db"`
	Collection string `bson:"collection"`
	EOF        bool   `bson:"EOF",omitempty`
}
type CollectionMetadata struct {
	Database   string `bson:"db"`
	Collection string `bson:"collection"`
	Metadata   string `bson:"metadata"`
}

const minBSONSize = 4 + 1 // an empty bson document should be exactly five bytes long

const delimiter int32 = -1
const delimiterBytes = []byte{255, 255, 255, 255}
