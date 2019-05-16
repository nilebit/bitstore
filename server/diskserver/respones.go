package diskserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path"
	"strconv"
	"strings"
)

func writeJson(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) (err error) {
	var bytes []byte
	if r.FormValue("pretty") != "" {
		bytes, err = json.MarshalIndent(obj, "", "  ")
	} else {
		bytes, err = json.Marshal(obj)
	}
	if err != nil {
		return
	}
	callback := r.FormValue("callback")
	if callback == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		_, err = w.Write(bytes)
	} else {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(httpStatus)
		if _, err = w.Write([]uint8(callback)); err != nil {
			return
		}
		if _, err = w.Write([]uint8("(")); err != nil {
			return
		}
		fmt.Fprint(w, string(bytes))
		if _, err = w.Write([]uint8(")")); err != nil {
			return
		}
	}

	return
}

func writeJsonError(w http.ResponseWriter, r *http.Request, httpStatus int, err error) {
	m := make(map[string]interface{})
	m["error"] = err.Error()
	if err := writeJson(w, r, httpStatus, m); err != nil {
		glog.V(0).Infof("error writing JSON %s: %v", m, err)
	}
}

// httpRange specifies the byte range to be sent to the client.
type httpRange struct {
	start, length int64
}

func (r httpRange) contentRange(size int64) string {
	return fmt.Sprintf("bytes %d-%d/%d", r.start, r.start+r.length-1, size)
}

func (r httpRange) mimeHeader(contentType string, size int64) textproto.MIMEHeader {
	return textproto.MIMEHeader{
		"Content-Range": {r.contentRange(size)},
		"Content-Type":  {contentType},
	}
}

// parseRange parses a Range header string as per RFC 2616.
func parseRange(s string, size int64) ([]httpRange, error) {
	if s == "" {
		return nil, nil // header not present
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, errors.New("invalid range")
	}
	var ranges []httpRange
	for _, ra := range strings.Split(s[len(b):], ",") {
		ra = strings.TrimSpace(ra)
		if ra == "" {
			continue
		}
		i := strings.Index(ra, "-")
		if i < 0 {
			return nil, errors.New("invalid range")
		}
		start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
		var r httpRange
		if start == "" {
			// If no start is specified, end specifies the
			// range start relative to the end of the file.
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil {
				return nil, errors.New("invalid range")
			}
			if i > size {
				i = size
			}
			r.start = size - i
			r.length = size - r.start
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i > size || i < 0 {
				return nil, errors.New("invalid range")
			}
			r.start = i
			if end == "" {
				// If no end is specified, range extends to end of the file.
				r.length = size - r.start
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.start > i {
					return nil, errors.New("invalid range")
				}
				if i >= size {
					i = size - 1
				}
				r.length = i - r.start + 1
			}
		}
		ranges = append(ranges, r)
	}
	return ranges, nil
}

func sumRangesSize(ranges []httpRange) (size int64) {
	for _, ra := range ranges {
		size += ra.length
	}
	return
}

// countingWriter counts how many bytes have been written to it.
type countingWriter int64

func (w *countingWriter) Write(p []byte) (n int, err error) {
	*w += countingWriter(len(p))
	return len(p), nil
}

// rangesMIMESize returns the number of bytes it takes to encode the
// provided ranges as a multipart response.
func rangesMIMESize(ranges []httpRange, contentType string, contentSize int64) (encSize int64) {
	var w countingWriter
	mw := multipart.NewWriter(&w)
	for _, ra := range ranges {
		mw.CreatePart(ra.mimeHeader(contentType, contentSize))
		encSize += ra.length
	}
	mw.Close()
	encSize += int64(w)
	return
}

func writeResponseContent(filename, mimeType string, rs io.ReadSeeker, w http.ResponseWriter, r *http.Request) error {
	var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")
	totalSize, e := rs.Seek(0, 2)
	if mimeType == "" {
		if ext := path.Ext(filename); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	if filename != "" {
		contentDisposition := "inline"
		if r.FormValue("dl") != "" {
			if dl, _ := strconv.ParseBool(r.FormValue("dl")); dl {
				contentDisposition = "attachment"
			}
		}
		w.Header().Set("Content-Disposition", contentDisposition+`; filename="`+fileNameEscaper.Replace(filename)+`"`)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return nil
	}
	rangeReq := r.Header.Get("Range")
	if rangeReq == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		if _, e = rs.Seek(0, 0); e != nil {
			return e
		}
		_, e = io.Copy(w, rs)
		return e
	}

	//the rest is dealing with partial content request
	//mostly copy from src/pkg/net/http/fs.go
	ranges, err := parseRange(rangeReq, totalSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return nil
	}
	if sumRangesSize(ranges) > totalSize {
		// The total number of bytes in all the ranges
		// is larger than the size of the file by
		// itself, so this is probably an attack, or a
		// dumb client.  Ignore the range request.
		return nil
	}
	if len(ranges) == 0 {
		return nil
	}
	if len(ranges) == 1 {
		// RFC 2616, Section 14.16:
		// "When an HTTP message includes the content of a single
		// range (for example, a response to a request for a
		// single range, or to a request for a set of ranges
		// that overlap without any holes), this content is
		// transmitted with a Content-Range header, and a
		// Content-Length header showing the number of bytes
		// actually transferred.
		// ...
		// A response to a request for a single range MUST NOT
		// be sent using the multipart/byteranges media type."
		ra := ranges[0]
		w.Header().Set("Content-Length", strconv.FormatInt(ra.length, 10))
		w.Header().Set("Content-Range", ra.contentRange(totalSize))
		w.WriteHeader(http.StatusPartialContent)
		if _, e = rs.Seek(ra.start, 0); e != nil {
			return e
		}

		_, e = io.CopyN(w, rs, ra.length)
		return e
	}
	// process multiple ranges
	for _, ra := range ranges {
		if ra.start > totalSize {
			http.Error(w, "Out of Range", http.StatusRequestedRangeNotSatisfiable)
			return nil
		}
	}
	sendSize := rangesMIMESize(ranges, mimeType, totalSize)
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	w.Header().Set("Content-Type", "multipart/byteranges; boundary="+mw.Boundary())
	sendContent := pr
	defer pr.Close() // cause writing goroutine to fail and exit if CopyN doesn't finish.
	go func() {
		for _, ra := range ranges {
			part, e := mw.CreatePart(ra.mimeHeader(mimeType, totalSize))
			if e != nil {
				pw.CloseWithError(e)
				return
			}
			if _, e = rs.Seek(ra.start, 0); e != nil {
				pw.CloseWithError(e)
				return
			}
			if _, e = io.CopyN(part, rs, ra.length); e != nil {
				pw.CloseWithError(e)
				return
			}
		}
		mw.Close()
		pw.Close()
	}()
	if w.Header().Get("Content-Encoding") == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(sendSize, 10))
	}
	w.WriteHeader(http.StatusPartialContent)
	_, e = io.CopyN(w, sendContent, sendSize)
	return e
}