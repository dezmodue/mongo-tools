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
type ArchiveHeader struct {
	ConcurrentCollections int32  `bson:"concurrent_collections",omitempty`
	ArchiveFormatVersion  string `bson:"version"`
}

type ArchivePrelude struct {
	Header                  *ArchiveHeader
	CollectionMetadatas     []*CollectionMetadata
	DBS                     []string
	CollectionMetadatasByDB map[string][]*CollectionMetadata
}

const minBSONSize = 4 + 1 // an empty bson document should be exactly five bytes long

var terminator int32 = -1
var terminatorBytes []byte = []byte{0xFF, 0xFF, 0xFF, 0xFF} // TODO, rectify this with terminator

const MagicNumber int32 = 0x6de9818b
