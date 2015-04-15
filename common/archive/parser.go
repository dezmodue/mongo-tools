package archive

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	"io"
)

type ParserConsumer interface {
	HandleOutOfBandBSON([]byte) error
	DispatchBSON([]byte) error
	End() error
}

type Parser struct {
	in       io.Reader
	consumer ParserConsumer
	buf      [db.MaxBSONSize]byte
	length   int
}

func NewArchiveParser(in io.Reader, consumer ParserConsumer) *Parser {
	return &Parser{
		in:       in,
		consumer: consumer,
	}
}

func (parse *Parser) readBSONOrDelimiter() (bool, error) {
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
	if size == delimiter {
		return true, nil
	}
	if size < minBSONSize || size > db.MaxBSONSize {
		return false, fmt.Errorf("Corruption found in archive; %v is neither a valid bson length nor a archive delimiter", size)
	}
	// TODO Because we're reusing this same buffer for all of our IO, we are basically guaranteeing that we'll copy the bytes twice
	// At some point we should fix this. It's slightly complex, because we'll need consumer methods closing one buffer and acquiring another
	_, err = io.ReadAtLeast(parse.in, parse.buf[4:size], int(size)-4)
	if err != nil {
		return false, err
	}
	if parse.buf[size-1] != 0x00 {
		return false, fmt.Errorf("Corruption found in archive; bson doesn't end with a null byte")
	}
	parse.length = int(size)
	return false, nil
}

func (parse *Parser) Run() (err error) {
	for {
		delimiter, err := parse.readBSONOrDelimiter()
		if err == io.EOF {
			err = parse.consumer.End()
			return err
		}
		if err != nil {
			return err
		}
		if delimiter {
			delimiter, err = parse.readBSONOrDelimiter()
			if err != nil { // all errors, including EOF are errors here
				return err
			}
			if delimiter {
				return fmt.Errorf("Error parsing archive; consecutive delimiters are not allowed")
			}
			err = parse.consumer.HandleOutOfBandBSON(parse.buf[:parse.length])
			if err != nil {
				return err
			}
		} else {
			err = parse.consumer.DispatchBSON(parse.buf[:parse.length])
			if err != nil {
				return err
			}
		}
	}
}
