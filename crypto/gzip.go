package crypto

import (
	"bytes"
	"compress/gzip"
	"io"
)

// GzipCompress Thx to code from @lz520520
func GzipCompress(src []byte) []byte {
	var in bytes.Buffer
	w := gzip.NewWriter(&in)
	_, _ = w.Write(src)
	_ = w.Close()
	return in.Bytes()
}

func GzipDecompress(src []byte) []byte {
	dst := make([]byte, 0)
	br := bytes.NewReader(src)
	gr, err := gzip.NewReader(br)
	if err != nil {
		return dst
	}
	defer func() {
		_ = gr.Close()
	}()
	tmp, err := io.ReadAll(gr)
	if err != nil {
		return dst
	}
	dst = tmp
	return dst
}
