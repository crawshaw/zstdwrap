package zstdwrap_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/crawshaw/zstdwrap"
)

func Test(t *testing.T) {
	src := strings.Repeat("Hello, World!\n", 20)
	var dst []byte

	t.Run("Compress", func(t *testing.T) {
		c, err := zstdwrap.NewCompressor(&zstdwrap.COptions{
			CompressionLevel: 15,
			Checksum:         true,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Delete()

		dst, err = c.Compress(nil, []byte(src))
		if err != nil {
			t.Fatal(err)
		}
		if len(dst) > len(src) {
			t.Errorf("compression made basic text larger: len(dst)=%d, len(src)=%d", len(dst), len(src))
		}
		if len(dst) < 6 {
			t.Fatal("compressed content too small")
		}
		// A zstd frame starts with a magic number
		// defined in RFC 8478 section 3.1.1.
		magic := []byte{0x28, 0xB5, 0x2F, 0xFD}
		if !bytes.Equal(dst[:4], magic) {
			t.Errorf("bad magic: %x want %x", dst[:4], magic)
		}

		// RFC 8478 section 3.1.1.1.1.
		frameHeaderDescriptor := dst[4]
		hasChecksum := frameHeaderDescriptor&0x4 != 0
		if !hasChecksum {
			t.Error("missing checksum")
		}
	})

	t.Run("Decompress", func(t *testing.T) {
		d, err := zstdwrap.NewDecompressor(0)
		if err != nil {
			t.Fatal(err)
		}
		dst2, err := d.Decompress(nil, dst)
		if err != nil {
			t.Fatal(err)
		}
		roundtrip := string(dst2)
		if roundtrip != src {
			t.Errorf("compress-then-decompress does not match original: %q", roundtrip)
		}
	})
}
