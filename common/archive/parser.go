package archive

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	"io"
	//	"os"
)

type ParserConsumer interface {
	HeaderBSON([]byte) error
	BodyBSON([]byte) error
	End() error
}

type Parser struct {
	in     io.Reader
	buf    [db.MaxBSONSize]byte
	length int
}

var errPrefix = "Corruption found in archive"

func (parse *Parser) readBSONOrTerminator() (bool, error) {
	parse.length = 0
	_, err := io.ReadAtLeast(parse.in, parse.buf[0:4], 4)
	if err != nil {
		return false, err
	}
	size := int32(
		(uint32(parse.buf[0]) << 0) |
			(uint32(parse.buf[1]) << 8) |
			(uint32(parse.buf[2]) << 16) |
			(uint32(parse.buf[3]) << 24),
	)
	if size == terminator {
		return true, nil
	}
	if size < minBSONSize || size > db.MaxBSONSize {
		return false, fmt.Errorf("%v; %v is neither a valid bson length nor a archive terminator", errPrefix, size)
	}
	// TODO Because we're reusing this same buffer for all of our IO, we are basically guaranteeing that we'll
	// copy the bytes twice.  At some point we should fix this. It's slightly complex, because we'll need consumer
	// methods closing one buffer and acquiring another
	_, err = io.ReadAtLeast(parse.in, parse.buf[4:size], int(size)-4)
	if err != nil {
		return false, err
	}
	if parse.buf[size-1] != 0x00 {
		return false, fmt.Errorf("%v; bson doesn't end with a null byte", errPrefix)
	}
	parse.length = int(size)
	return false, nil
}

func (parse *Parser) ReadAllBlocks(consumer ParserConsumer) (err error) {
	for err == nil {
		err = parse.ReadBlock(consumer)
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (parse *Parser) ReadBlock(consumer ParserConsumer) (err error) {
	terminator, err := parse.readBSONOrTerminator()
	if err == io.EOF {
		handlerErr := consumer.End()
		if handlerErr != nil {
			return handlerErr
		}
		return err
	}
	if err != nil {
		return fmt.Errorf("%v; %v", errPrefix, err)
	}
	if terminator {
		return fmt.Errorf("%v; consecutive terminators / empty blocks are not allowed", errPrefix)
	}
	err = consumer.HeaderBSON(parse.buf[:parse.length])
	if err != nil {
		return err
	}
	for {
		terminator, err = parse.readBSONOrTerminator()
		if err != nil { // all errors, including EOF are errors here
			return fmt.Errorf("%v; %v", err)
		}
		if terminator {
			return nil
		}
		err = consumer.BodyBSON(parse.buf[:parse.length])
		if err != nil {
			return fmt.Errorf("%v; %v", errPrefix, err)
		}
	}
}
