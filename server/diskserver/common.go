package diskserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/golang/glog"
)

const (
	StartLogTemplate         = "begin: host=[%s] remoteRealIP=[%s] method=[%s] url=[%s] start_time=[%s] form_data=[%s] \n"
	EndLogTemplate           = "end: host=[%s] remoteRealIP=[%s] method=[%s] url=[%s] status=[%d] end_time=[%s] process_time=[%d] form_data=[%s] header=[%s] errmsg=[%v] \n"
	TimeFormat               = "2006-01-02 15:04:05"
	defaultMaxMemory   int64 = 32 << 20 // 32 MB
	RespBodyTypeJson         = "json"
	RespBodyTypeString       = "string"
)

type Context struct {
	RemoteRealIP   string
	Host           string
	Start          time.Time
	RespBodyType   string
	RespBody       string
	RespBodyJson   interface{}
	ProcessTime    int64
	FormValues     string
	Request        *http.Request
	RespHttpStatus int
	Err            error
}

// get hostname
func hostName() string {
	host, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return host
}

func NewContext(r *http.Request) *Context {
	var realIP string
	if len(r.Header[HeaderXRealIP]) > 0 {
		realIP = r.Header[HeaderXRealIP][0]
	}
	if realIP == "" {
		realIP = r.Header.Get(HeaderXRealIP)
	}

	r.ParseMultipartForm(defaultMaxMemory)
	data, _ := json.Marshal(r.Form)

	ctx := &Context{
		RemoteRealIP:   realIP,
		Host:           hostName(),
		Start:          time.Now(),
		Request:        r,
		FormValues:     string(data),
		RespBodyType:   RespBodyTypeJson,
		RespHttpStatus: http.StatusOK,
	}

	glog.V(0).Infof(StartLogTemplate, ctx.Host, ctx.RemoteRealIP, ctx.Request.Method, ctx.Request.RequestURI, ctx.Start.Format(TimeFormat), ctx.FormValues)
	return ctx
}

func Summary(w http.ResponseWriter, ctx *Context) {
	err := SendResponse(w, ctx)
	if err != nil {
		errLog := fmt.Errorf("%v. %s", ctx.Err, err.Error())
		ctx.Err = errLog
	}
	Logger(ctx)
}

// service log
func Logger(ctx *Context) {
	ctx.ProcessTime = time.Since(ctx.Start).Nanoseconds() / time.Millisecond.Nanoseconds()
	// print request header
	data, _ := json.Marshal(ctx.Request.Header)

	glog.V(0).Infof(EndLogTemplate, ctx.Host, ctx.RemoteRealIP, ctx.Request.Method, ctx.Request.RequestURI, ctx.RespHttpStatus, time.Now().Format(TimeFormat),
		ctx.ProcessTime, ctx.FormValues, string(data), ctx.Err)
}

func SendResponse(w http.ResponseWriter, ctx *Context) error {
	if ctx.Err == nil {
		if ctx.RespBodyType == RespBodyTypeJson {
			return writeJsonQuiet(w, ctx.Request, ctx.RespHttpStatus, ctx.RespBodyJson)
		}
		return writeResponse(w, ctx.Request, ctx.RespHttpStatus, []byte(ctx.RespBody))
	} else {
		return writeJsonError(w, ctx.Request, ctx.RespHttpStatus, ctx.Err)
	}
}
