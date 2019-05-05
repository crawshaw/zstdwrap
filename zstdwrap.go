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
// #include "zstd_errors.h"
import "C"
import (
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/xerrors"
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

// Decompress decompresse the contents of src into dst, and returns the new dst.
//
// Decompress requires the frame being decompressed be smaller
// than cap(dst) or be smaller than the Decompressor's maximum
// window log.
//
// The len(src) must be exactly equal to the byte length of one
// or more frames.
func (d *Decompressor) Decompress(dst, src []byte) ([]byte, error) {
	if src == nil {
		return nil, errors.New("zstdwrap.Decompress: nil src")
	}
	if dst != nil {
		dst = dst[:cap(dst)]
	}
	if contentSize, err := FrameContentSize(src); err != nil {
		return nil, xerrors.Errorf("zstdwrap.Decompress: %w", err)
	} else if contentSize > int64(d.windowLogMax) {
		return nil, xerrors.Errorf("zstdwrap.Decompress: frame too big: %d", contentSize)
	} else if int(contentSize) > len(dst) {
		dst = append(dst, make([]byte, int(contentSize)-len(dst))...)
	}

	dstv := unsafe.Pointer(&dst[0])
	srcv := unsafe.Pointer(&src[0])
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

var ErrContentSizeUnknown = errors.New("zstdwrap: unknown frame content size")
var ErrBadFrame = errors.New("zstdwrap: bad frame")

// FrameContentSize reports the decompressed size of a frame's content.
func FrameContentSize(src []byte) (int64, error) {
	var srcv unsafe.Pointer
	if src != nil {
		srcv = unsafe.Pointer(&src[0])
	}
	sz := C.ZSTD_getFrameContentSize(srcv, C.size_t(len(src)))
	if sz == C.ZSTD_CONTENTSIZE_UNKNOWN {
		return 0, ErrContentSizeUnknown
	} else if sz == C.ZSTD_CONTENTSIZE_ERROR {
		return 0, ErrBadFrame
	}
	return int64(sz), nil
}

// FrameCompressedSize reports the size of a frame.
// For the reported value n, buf[:n] is a valid src for Decompress.
func FrameCompressedSize(buf []byte) (n int, err error) {
	var bufv unsafe.Pointer
	if buf != nil {
		bufv = unsafe.Pointer(&buf[0])
	}
	res := C.ZSTD_findFrameCompressedSize(bufv, C.size_t(len(buf)))
	if err := isErr("", res); err != nil {
		// no location so ErrCodeSrcSizeWrong is zero-alloc
		return 0, err
	}
	return int(res), nil
}

func isErr(loc string, res C.size_t) error {
	code := int(C.ZSTD_getErrorCode(res))
	if code == 0 {
		return nil
	}
	if loc == "" {
		return errCode(code)
	}
	return xerrors.Errorf("zstdwrap.%s: %w", loc, errCode(code))
}

type ErrorCode int

func (code *ErrorCode) Error() string {
	if code == nil {
		return "zstdwrap.ErrorCode(nil)"
	}
	return C.GoString(C.ZSTD_getErrorString(C.ZSTD_ErrorCode(*code)))
}

// Zstd stable error codes.
var (
	ErrGeneric                      = errCode(1)
	ErrPrefixUnknown                = errCode(10)
	ErrVersionUnsupported           = errCode(12)
	ErrFrameParameterUnsupported    = errCode(14)
	ErrFrameParameterWindowTooLarge = errCode(16)
	ErrCorruptionDetected           = errCode(20)
	ErrChecksumWrong                = errCode(22)
	ErrDictionaryCorrupted          = errCode(30)
	ErrDictionaryWrong              = errCode(32)
	ErrDictionaryCreationFailed     = errCode(34)
	ErrParameterUnsupported         = errCode(40)
	ErrParameterOutOfBound          = errCode(42)
	ErrTableLogTooLarge             = errCode(44)
	ErrMaxSymbolValueTooLarge       = errCode(46)
	ErrMaxSymbolValueTooSmall       = errCode(48)
	ErrStageWrong                   = errCode(60)
	ErrInitMissing                  = errCode(62)
	ErrMemoryAllocation             = errCode(64)
	ErrWorkSpaceTooSmall            = errCode(66)
	ErrDstSizeTooSmall              = errCode(70)
	ErrSrcSizeWrong                 = errCode(72)
	ErrDstBufferNull                = errCode(74)
)

func errCode(code int) *ErrorCode {
	if e := knownErrCodes[code]; e != nil {
		return e
	}
	e := ErrorCode(code)
	knownErrCodes[code] = &e
	return &e
}

// Avoid allocating errors.
var knownErrCodes = make(map[int]*ErrorCode)
