package archive

import (
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"io"
)

func ReadPrelude(in io.Reader) (*ArchivePrelude, error) {
	prelude := &ArchivePrelude{}
	magicNumberBuf := make([]byte, 4)
	_, err := io.ReadAtLeast(in, magicNumberBuf, 4)
	if err != nil {
		return nil, err
	}
	magicNumber := int32(
		(uint32(magicNumberBuf[0]) << 0) |
			(uint32(magicNumberBuf[1]) << 8) |
			(uint32(magicNumberBuf[2]) << 16) |
			(uint32(magicNumberBuf[3]) << 24),
	)

	if magicNumber != MagicNumber {
		return nil, fmt.Errorf("stream or file does not apear to be a mongodump archive")
	}

	parser := Parser{In: in}
	parserConsumer := &preludeParserConsumer{prelude: prelude}
	err = parser.ReadBlock(parserConsumer)
	if err != nil {
		return nil, err
	}
	prelude.CollectionMetadatasByDB = make(map[string][]*CollectionMetadata, 0)
	for _, cm := range prelude.CollectionMetadatas {
		_, ok := prelude.CollectionMetadatasByDB[cm.Database]
		if !ok {
			prelude.DBS = append(prelude.DBS, cm.Database)
		}
		prelude.CollectionMetadatasByDB[cm.Database] = append(prelude.CollectionMetadatasByDB[cm.Database], cm)
	}

	return prelude, nil

}

type preludeParserConsumer struct {
	prelude *ArchivePrelude
}

func (hpc *preludeParserConsumer) HeaderBSON(data []byte) error {
	hpc.prelude.Header = &ArchiveHeader{}
	err := bson.Unmarshal(data, hpc.prelude.Header)
	if err != nil {
		return err
	}
	return nil
}
func (hpc *preludeParserConsumer) BodyBSON(data []byte) error {
	cm := &CollectionMetadata{}
	err := bson.Unmarshal(data, cm)
	if err != nil {
		return err
	}
	hpc.prelude.CollectionMetadatas = append(hpc.prelude.CollectionMetadatas, cm)
	return nil
}

func (hpc *preludeParserConsumer) End() error {
	return nil
}

func WritePrelude(out io.Writer, prelude *ArchivePrelude) error {
	magicNumberBytes := make([]byte, 4)
	for i, _ := range magicNumberBytes {
		magicNumberBytes[i] = byte(uint32(MagicNumber) >> uint(i*8))
	}
	_, err := out.Write(magicNumberBytes)
	if err != nil {
		return err
	}
	buf, err := bson.Marshal(prelude.Header)
	if err != nil {
		return err
	}
	_, err = out.Write(buf)
	if err != nil {
		return err
	}
	for _, cm := range prelude.CollectionMetadatas {
		buf, err = bson.Marshal(cm)
		if err != nil {
			return err
		}
		_, err = out.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = out.Write(terminatorBytes)
	if err != nil {
		return err
	}
	return nil
}
