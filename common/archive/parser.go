package archive

import (
	"gopkg.in/mgo.v2/bson"
	"io"
)

const minBSONSize = 4 + 1 // an empty bson document should be exactly five bytes long

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

func NewArchiveParser(in io.Reader, consumer ParserConsumer) {
	return &Parser{
		in:       in,
		consumer: consumer,
	}
}

func (parse *Parser) readBSONOrDelimiter() (int, bool, error) {
	parse.length = 0
	_, err := io.ReadAtLeast(parse.in, buf[0:4], 4)
	if err != nil {
		return false, err
	}
	size := int32(
		(uint32(buf[0]) << 0) |
			(uint32(buf[1]) << 8) |
			(uint32(buf[2]) << 16) |
			(uint32(buf[3]) << 24),
	)
	if size == delimiter {
		return true, nil
	}
	if size < minBSONSize || size > db.MaxBSONSize {
		return false, fmt.Errorf("Corruption found in archive; %v is neither a valid bson length nor a archive delimiter", size)
	}
	// TODO Because we're reusing this same buffer for all of our IO, we are basically guaranteeing that we'll copy the bytes twice
	// At some point we should fix this. It's slightly complex, because we'll need consumer methods closing one buffer and acquiring another
	bsonLength, err := io.ReadAtLeast(parse.in, buf[4:size], size-4)
	if err != nil {
		return false, err
	}
	parse.length = size
	return false, nil
}

func (parse *Parser) run() (err error) {
	for {
		delimiter, err := parse.readBSONOrDelimiter()
		if err == EOF {
			err = parse.End()
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
			err = parse.handleOutOfBandBSON()
			if err != nil {
				return err
			}
		} else {
			err = parse.dispatchBSON()
			if err != nil {
				return err
			}
		}
	}
}

func (parse *Parser) handleOutOfBandBSON() error {
	return parse.consumer.HandleOutOfBandBSON(parse.buf, parse.length)
}

func (parse *Parser) dispatchBson() error {
	return parse.consumer.DispatchBSON(parse.buf, parse.length)
}
