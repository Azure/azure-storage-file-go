package azfile

import (
	"errors"
	"io"
)

const (
	// CountToEnd indiciates a flag for count parameter. It means the count of bytes
	// from start offset to the end of file.
	CountToEnd = -1
)

func validateSeekableStreamAt0AndGetCount(body io.ReadSeeker) int64 {
	if body == nil { // nil body's are "logically" seekable to 0 and are 0 bytes long
		return 0
	}
	validateSeekableStreamAt0(body)
	count, err := body.Seek(0, io.SeekEnd)
	if err != nil {
		panic("failed to seek stream")
	}
	body.Seek(0, io.SeekStart)
	return count
}

func validateSeekableStreamAt0(body io.ReadSeeker) {
	if body == nil { // nil body's are "logically" seekable to 0
		return
	}
	if pos, err := body.Seek(0, io.SeekCurrent); pos != 0 || err != nil {
		if err != nil {
			panic(err)
		}
		panic(errors.New("stream must be set to position 0"))
	}
}
