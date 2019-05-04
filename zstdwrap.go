// Copyright (c) 2019 David Crawshaw <david@zentus.com>
//
// Permission to use, copy, modify, and distribute this software for any
// purpose with or without fee is hereby granted, provided that the above
// copyright notice and this permission notice appear in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
// WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
// ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
// WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
// ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
// OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

// Package zstdwrap provides a low-level cgo wrapper over zstd.
//
// The goal is not to implement a typical Go compression API
// of io.Reader and io.Writer. Instead this package is nothing
// more than type-safe primitives.
//
// TODO: streaming interface
package zstdwrap

// #cgo CFLAGS: -DZSTD_MULTITHREAD
// #cgo linux LDFLAGS: -pthread
// #cgo darwin LDFLAGS: -pthread
//
// #define ZSTD_STATIC_LINKING_ONLY
// #include "zstd.h"
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

type COptions struct {
	CompressionLevel int // 1-22, default 3, caution using levels >= 20
	Checksum         bool
	// TODO dictionary
}

type Compressor struct {
	ctx *C.ZSTD_CCtx
}

func NewCompressor(opts *COptions) (*Compressor, error) {
	c := &Compressor{
		ctx: C.ZSTD_createCCtx(),
	}
	if c.ctx == nil {
		return nil, fmt.Errorf("zstdwrap: ZSTD_createCCtx failed")
	}
	if opts != nil {
		if l := opts.CompressionLevel; l != 0 {
			res := C.ZSTD_CCtx_setParameter(c.ctx, C.ZSTD_c_compressionLevel, C.int(l))
			if err := isErr("NewCompressor(level)", res); err != nil {
				return nil, err
			}
		}
		if opts.Checksum {
			res := C.ZSTD_CCtx_setParameter(c.ctx, C.ZSTD_c_checksumFlag, 1)
			if err := isErr("NewCompressor(checksum)", res); err != nil {
				return nil, err
			}
		}
	}
	return c, nil
}

func (c *Compressor) Delete() error {
	err := isErr("Delete", C.ZSTD_freeCCtx(c.ctx))
	c.ctx = nil
	return err
}

// Compress compresses the contents of src into dst, and returns the new dst.
//
// If cap(dst) < CompressBound(len(src)), then memory will be allocated.
//
// Always builds a complete frame.
// Equivalent to ZSTD_compress2.
func (c *Compressor) Compress(dst, src []byte) ([]byte, error) {
	if need := CompressBound(len(src)); cap(dst) < need {
		// Efficient as of Go 1.11:
		// https://golang.org/doc/go1.11#performance-compiler
		dst = append(dst, make([]byte, need-len(dst))...)
	} else {
		dst = dst[:need]
	}

	dstv := unsafe.Pointer(&dst[0])
	srcv := unsafe.Pointer(&src[0])
	res := C.ZSTD_compress2(c.ctx, dstv, C.size_t(len(dst)), srcv, C.size_t(len(src)))
	if err := isErr("Compress", res); err != nil {
		return nil, err
	}
	dst = dst[:int(res)]
	return dst, nil
}

func CompressBound(srcSize int) int {
	// TODO: this is a one-line macro. Implement directly in Go.
	return int(C.ZSTD_compressBound(C.size_t(srcSize)))
}

type DOptions struct {
	WindowLogMax int // 0 default, otherwise must be power of 2
}

type Decompressor struct {
	ctx          *C.ZSTD_DCtx
	windowLogMax int
}

// NewDecompressor creates a Decompressor.
//
// The maximum frame that can be decompressed is windowLogMax,
// which must be a power of two.
// If zero, the default is 1<<ZSTD_WINDOWLOG_LIMIT_DEFAULT (128mb).
func NewDecompressor(windowLogMax int) (*Decompressor, error) {
	d := &Decompressor{
		ctx:          C.ZSTD_createDCtx(),
		windowLogMax: windowLogMax,
	}
	if d.ctx == nil {
		return nil, fmt.Errorf("zstdwrap: ZSTD_createDCtx failed")
	}
	if d.windowLogMax == 0 {
		d.windowLogMax = int(1 << C.ZSTD_WINDOWLOG_LIMIT_DEFAULT)
	} else {
		res := C.ZSTD_DCtx_setParameter(d.ctx, C.ZSTD_d_windowLogMax, C.int(d.windowLogMax))
		if err := isErr("NewDecompressor(windowlog)", res); err != nil {
			return nil, err
		}
	}
	return d, nil
}

var ErrContentSizeUnknown = errors.New("zstdwrap: unknown frame content size")

// Decompress decompresse the contents of src into dst, and returns the new dst.
//
// Decompress requires the frame being decompressed be smaller
// than cap(dst) and smaller than the Decompressor's maximum window log.
// (Frames encoded in a single-pass, such as using this package's
// Compress method, contain a size. Frames that were stream encoded may
// not contain a size.)
//
// Always decompresses a complete frame or reports an error.
func (d *Decompressor) Decompress(dst, src []byte) ([]byte, error) {
	if dst != nil {
		dst = dst[:cap(dst)]
	}
	var srcv unsafe.Pointer
	if src != nil {
		srcv = unsafe.Pointer(&src[0])
	}
	frameSize := C.ZSTD_getFrameContentSize(srcv, C.size_t(len(src)))
	if frameSize == C.ZSTD_CONTENTSIZE_UNKNOWN {
		if dst == nil {
			return nil, ErrContentSizeUnknown
		}
	} else if frameSize == C.ZSTD_CONTENTSIZE_ERROR {
		if dst == nil {
			return nil, errors.New("zstdwrap.Decompress.Size: failed")
		}
	} else if int64(frameSize) > int64(len(dst)) {
		// A frame can be enormous (requiring a 64-bit representation).
		if int64(frameSize) > int64(d.windowLogMax) {
			return nil, fmt.Errorf("zstdwrap.Decompress: frame too big: %d (max: %d)", frameSize, d.windowLogMax)
		}
		dst = append(dst, make([]byte, int(frameSize)-len(dst))...)
	}
	dstv := unsafe.Pointer(&dst[0])
	res := C.ZSTD_decompressDCtx(d.ctx, dstv, C.size_t(len(dst)), srcv, C.size_t(len(src)))
	if err := isErr("Decompress", res); err != nil {
		return nil, err
	}
	dst = dst[:int(res)]
	return dst, nil
}

func (d *Decompressor) Delete() error {
	err := isErr("Delete", C.ZSTD_freeDCtx(d.ctx))
	d.ctx = nil
	return err
}

func isErr(loc string, res C.size_t) error {
	if C.ZSTD_isError(res) != 0 {
		return fmt.Errorf("zstdwrap.%s: %s", loc, C.GoString(C.ZSTD_getErrorName(res)))
	}
	return nil
}
