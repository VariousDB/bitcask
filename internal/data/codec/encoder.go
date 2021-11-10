package codec

import (
	"bufio"
	"encoding/binary"
	"io"

	"git.mills.io/prologic/bitcask/internal"
	"github.com/pkg/errors"
)

const (
	keySize      = 4
	valueSize    = 8
	checksumSize = 4
	ttlSize      = 8
	MetaInfoSize = keySize + valueSize + checksumSize + ttlSize
)

// NewEncoder creates a streaming Entry encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: bufio.NewWriter(w)}
}

// Encoder wraps an underlying io.Writer and allows you to stream
// Entry encodings on it.
type Encoder struct {
	w *bufio.Writer
}

// Encode takes any Entry and streams it to the underlying writer.
// Messages are framed with a key-length and value-length prefix.
func (e *Encoder) Encode(msg internal.Entry) (int64, error) {
	var bufKeyValue = make([]byte, keySize+valueSize)
	// bufio的write都是写缓存
	// 放入key，value的length
	binary.BigEndian.PutUint32(bufKeyValue[:keySize], uint32(len(msg.Key)))
	binary.BigEndian.PutUint64(bufKeyValue[keySize:keySize+valueSize], uint64(len(msg.Value)))
	// 调用writer的写方法
	if _, err := e.w.Write(bufKeyValue); err != nil {
		return 0, errors.Wrap(err, "failed writing key & value length prefix")
	}
	// 写入entry的key，value
	if _, err := e.w.Write(msg.Key); err != nil {
		return 0, errors.Wrap(err, "failed writing key data")
	}
	if _, err := e.w.Write(msg.Value); err != nil {
		return 0, errors.Wrap(err, "failed writing value data")
	}
	// 写入checksum
	bufChecksumSize := bufKeyValue[:checksumSize]
	binary.BigEndian.PutUint32(bufChecksumSize, msg.Checksum)
	if _, err := e.w.Write(bufChecksumSize); err != nil {
		return 0, errors.Wrap(err, "failed writing checksum data")
	}
	// 写入过期时间
	bufTTL := bufKeyValue[:ttlSize]
	if msg.Expiry == nil {
		binary.BigEndian.PutUint64(bufTTL, uint64(0))
	} else {
		binary.BigEndian.PutUint64(bufTTL, uint64(msg.Expiry.Unix()))
	}
	if _, err := e.w.Write(bufTTL); err != nil {
		return 0, errors.Wrap(err, "failed writing ttl data")
	}
	// 调用flush，将缓存内的数据落盘
	if err := e.w.Flush(); err != nil {
		return 0, errors.Wrap(err, "failed flushing data")
	}

	return int64(keySize + valueSize + len(msg.Key) + len(msg.Value) + checksumSize + ttlSize), nil
}
