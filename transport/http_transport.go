// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package transport

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"golang.org/x/net/context"

	"github.com/ninibe/bigduration"
	"github.com/ninibe/netlog"
	"github.com/ninibe/netlog/message"
)

// NewHTTPTransport transport sets up an HTTP interface around a NetLog.
func NewHTTPTransport(nl *netlog.NetLog) *HTTPTransport {
	return &HTTPTransport{nl: nl}
}

// HTTPTransport implements an HTTP server around a NetLog.
type HTTPTransport struct {
	nl *netlog.NetLog
}

// ServeHTTP implements the http.Handler interface around a NetLog.
func (ht *HTTPTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	router := httprouter.New()
	router.GET("/", ht.handleServerInfo)
	router.GET("/:topic", ht.handleTopicInfo)
	router.POST("/:topic", ht.handleCreateTopic)
	router.POST("/:topic/payload", ht.handleWritePayload)
	router.GET("/:topic/payload/:offset", ht.handleReadPayload)
	router.GET("/:topic/sync", ht.handleSync)
	router.POST("/:topic/scanner", ht.handleCreateScanner)
	router.DELETE("/:topic/scanner", ht.handleDeleteScanner)
	router.GET("/:topic/scan", withCtx(ht.handleScanTopic))
	router.GET("/:topic/check", withCtx(ht.handleCheckTopic))
	router.DELETE("/:topic", ht.handleDeleteTopic)
	router.ServeHTTP(w, r)
	return
}

// ctxHandle is the signature a context-friendly http handler.
type ctxHandle func(context.Context, http.ResponseWriter, *http.Request, httprouter.Params)

// withCtx is a wrapper function to inject a context into an http handler
// the context gets canceled if the http connection is closed by the client.
func withCtx(handle ctxHandle) httprouter.Handle {
	ctx := context.Background()
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		clientGone := w.(http.CloseNotifier).CloseNotify()
		go func() {
			select {
			case <-ctx.Done():
			case <-clientGone:
				cancel()
			}
		}()

		handle(ctx, w, r, ps)
	}
}

func (ht *HTTPTransport) handleCreateTopic(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	var settings netlog.TopicSettings
	encoder := json.NewDecoder(r.Body)
	err := encoder.Decode(&settings)
	if err != nil && err != io.EOF {
		log.Print(err)
		JSONErrorResponse(w, netlog.ErrBadRequest)
		return
	}

	_, err = ht.nl.CreateTopic(ps.ByName("topic"), settings)
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	JSONOKResponse(w, "topic created")
}

func (ht *HTTPTransport) handleReadPayload(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	t, err := ht.nl.Topic(ps.ByName("topic"))
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	defer logClose(r.Body)

	offset, err := t.ParseOffset(ps.ByName("offset"))
	if err != nil {
		JSONErrorResponse(w, netlog.ErrInvalidOffset)
		return
	}

	data, err := t.Payload(offset)
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(data)
	if err != nil {
		log.Printf("error: failed to write HTTP response %s", err)
	}
}

func (ht *HTTPTransport) handleWritePayload(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	t, err := ht.nl.Topic(ps.ByName("topic"))
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	defer logClose(r.Body)

	buf, err := ioutil.ReadAll(r.Body)
	if len(buf) == 0 || len(buf) < int(r.ContentLength) {
		JSONErrorResponse(w, netlog.ErrBadRequest)
		return
	}

	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	entry := message.MessageFromPayload(buf)
	buf = entry.Bytes()
	_, err = t.Write(buf)
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (ht *HTTPTransport) handleSync(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	t, err := ht.nl.Topic(ps.ByName("topic"))
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	err = t.Sync()
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	JSONOKResponse(w, "synced")
}

func (ht *HTTPTransport) handleTopicInfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	t, err := ht.nl.Topic(ps.ByName("topic"))
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	info, err := t.Info()
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	JSONResponse(w, info)
}

func (ht *HTTPTransport) handleServerInfo(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	JSONResponse(w, ht.nl.TopicList())
}

func (ht *HTTPTransport) handleDeleteTopic(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	force := trueStr(r.URL.Query().Get("force"))
	err := ht.nl.DeleteTopic(ps.ByName("topic"), force)
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	JSONOKResponse(w, "topic deleted")
}

func (ht *HTTPTransport) handleScanTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	t, err := ht.nl.Topic(ps.ByName("topic"))
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	sc, err := t.Scanner(r.URL.Query().Get("id"))
	if err != nil {
		JSONErrorResponse(w, netlog.ErrScannerNotFound)
		return
	}

	var timeout time.Duration
	var bd bigduration.BigDuration

	wait := r.URL.Query().Get("wait")
	if wait == "" {
		timeout = 5 * time.Millisecond
	} else {
		bd, err = bigduration.ParseBigDuration(wait)
		if err != nil {
			JSONErrorResponse(w, netlog.ErrInvalidDuration)
			return
		}

		timeout = bd.Duration()
	}

	ctx, _ = context.WithTimeout(ctx, timeout)
	m, o, err := sc.Scan(ctx)
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Add("X-offset", strconv.FormatInt(o, 10))
	w.Header().Add("X-crc32", strconv.FormatInt(int64(m.CRC32()), 10))

	_, err = w.Write(m.Payload())
	if err != nil {
		log.Printf("error: failed to write HTTP response %s", err)
	}
}

func (ht *HTTPTransport) handleCreateScanner(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	t, err := ht.nl.Topic(ps.ByName("topic"))
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	from, err := t.ParseOffset(r.URL.Query().Get("from"))
	if err != nil {
		JSONErrorResponse(w, netlog.ErrBadRequest)
		return
	}

	persist := trueStr(r.URL.Query().Get("persist"))
	ts, err := t.NewScanner(from, persist)
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	JSONResponse(w, ts.Info())
}

func (ht *HTTPTransport) handleDeleteScanner(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	t, err := ht.nl.Topic(ps.ByName("topic"))
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	ID := r.URL.Query().Get("id")
	if ID == "" {
		JSONErrorResponse(w, netlog.ErrBadRequest)
		return
	}

	err = t.DeleteScanner(ID)
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	JSONOKResponse(w, "scanner deleted")
}

func (ht *HTTPTransport) handleCheckTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	t, err := ht.nl.Topic(ps.ByName("topic"))
	if err != nil {
		JSONErrorResponse(w, err)
		return
	}

	from, err := t.ParseOffset(r.URL.Query().Get("from"))
	if err != nil {
		JSONErrorResponse(w, netlog.ErrBadRequest)
		return
	}

	iErrs, err := t.CheckIntegrity(ctx, from)
	if err != nil {
		JSONErrorResponse(w, netlog.ErrBadRequest)
		return
	}

	if len(iErrs) == 0 {
		JSONOKResponse(w, "topic healthy")
		return
	}

	JSONResponse(w, iErrs)
}

// IDMsg is the standard response when returning an ID
type IDMsg struct {
	ID string `json:"id"`
}

type successMsg struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}

// JSONErrorResponse is a convenience function to transform errors into JSON HTTP responses
func JSONErrorResponse(w http.ResponseWriter, err error) {
	err = netlog.ExtErr(err)
	if e, ok := err.(netlog.NLError); ok {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(e.StatusCode())
		encoder := json.NewEncoder(w)
		encodingError := encoder.Encode(e)
		if encodingError != nil {
			panic(encodingError)
		}

		level := "warn"
		if e == netlog.ErrUnknown {
			level = "alert"
		} else if e.StatusCode() >= 500 {
			level = "error"
		}

		log.Printf("%s: status %d -> %s", level, e.StatusCode(), e)
		return
	}

	log.Printf("alert: status 500 -> throwing unknown error: %s", err.Error())
	w.WriteHeader(http.StatusInternalServerError)
}

// JSONResponse is a convenience function to transform data into JSON HTTP responses
func JSONResponse(w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	err := json.NewEncoder(w).Encode(payload)
	if err != nil {
		panic(err)
	}
}

// JSONOKResponse is a convenience function to transform success messages into JSON HTTP responses
func JSONOKResponse(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	err := json.NewEncoder(w).Encode(successMsg{
		OK:      true,
		Message: message,
	})
	if err != nil {
		panic(err)
	}
}

func trueStr(s string) bool {
	s = strings.ToLower(s)
	return s == "1" || s == "true" || s == "yes"
}

// logClose calls Close on the subject and logs the error if any
// this is handy to call Close on defer
func logClose(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Printf("error: %s", err)
	}
}
