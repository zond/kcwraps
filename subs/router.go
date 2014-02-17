package subs

import (
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"time"
	"github.com/zond/diplicity/common"
	"github.com/zond/kcwraps/kol"

	"code.google.com/p/go.net/websocket"
)

const (
	Fatal = iota
	Error
	Info
	Debug
	Trace
)

type Route struct {
	Pattern *regexp.Regexp
}

type Routes []Route

func NewRouter(db *kol.DB) (result *Router) {
	result = &Router{
		DB:     db,
		Logger: log.New(os.Stdout, "", 0),
	}
	result.OnUnsubscribeFactory = result.DefaultOnUnsubscribeFactory
	result.EventLoggerFactory = result.DefaultEventLoggerFactory
	result.OnDisconnectFactory = result.DefaultOnDisconnectFactory
	return
}

type Router struct {
	Routes               Routes
	DB                   *kol.DB
	Logger               *log.Logger
	LogLevel             int
	OnUnsubscribeFactory func(ws *websocket.Conn) func(s *Subscription, reason interface{})
	EventLoggerFactory   func(ws *websocket.Conn) func(name string, i interface{}, op string, dur time.Duration)
	OnDisconnectFactory  func(ws *websocket.Conn) func()
}

func (self *Router) DefaultOnUnsubscribeFactory(ws *websocket.Conn) func(s *Subscription, reason interface{}) {
	return func(s *Subscription, reason interface{}) {
		self.Debugf("\t%v\t%v\t%v\t%v\t[unsubscribing]", ws.Request().URL.Path, ws.Request().RemoteAddr, s.URI(), reason)
		if self.LogLevel > Trace {
			self.Tracef("%s", debug.Stack())
		}
	}
}

func (self *Router) DefaultEventLoggerFactory(ws *websocket.Conn) func(name string, i interface{}, op string, dur time.Duration) {
	return func(name string, i interface{}, op string, dur time.Duration) {
		self.Debugf("\t%v\t%v\t%v\t%v\t%v ->", ws.Request().URL.Path, ws.Request().RemoteAddr, op, name, dur)
	}
}

func (self *Router) DefaultOnDisconnectFactory(ws *websocket.Conn) func() {
	return func() {
		self.Infof("\t%v\t%v -> [unsubscribing all]", ws.Request().URL.Path, ws.Request().RemoteAddr)
	}
}

func (self *Router) Logf(level int, format string, args ...interface{}) {
	if level <= self.LogLevel {
		log.Printf(format, args...)
	}
}

func (self *Router) Fatalf(format string, args ...interface{}) {
	self.Logf(Fatal, "\033[1;31mFATAL\t"+format+"\033[0m", args...)
}

func (self *Router) Errorf(format string, args ...interface{}) {
	self.Logf(Error, "\033[31mERROR\t"+format+"\033[0m", args...)
}

func (self *Router) Infof(format string, args ...interface{}) {
	self.Logf(Info, "INFO\t"+format, args...)
}

func (self *Router) Debugf(format string, args ...interface{}) {
	self.Logf(Debug, "\033[32mDEBUG\t"+format+"\033[0m", args...)
}

func (self *Router) Tracef(format string, args ...interface{}) {
	self.Logf(Trace, "\033[1;32mTRACE\t"+format+"\033[0m", args...)
}

func (self *Router) Path(exp string) (result *Route) {
	result = &Route{
		Pattern: regexp.MustCompile(exp),
	}
	return
}

func (self *Router) handleMessage(ws *websocket.Conn, pack *Pack, message *Message) (err error) {
	return
}

func (self *Router) handleConnection(ws *websocket.Conn) {
	self.Infof("\t%v\t%v <-", ws.Request().URL, ws.Request().RemoteAddr)
	defer self.OnDisconnectFactory(ws)()

	pack := NewPack(self.DB, ws).OnUnsubscribe(self.OnUnsubscribeFactory(ws)).Logger(self.EventLoggerFactory(ws))
	defer pack.UnsubscribeAll()

	var start time.Time
	for {
		var message Message
		if err := websocket.JSON.Receive(ws, &message); err == nil {
			start = time.Now()
			if err = self.handleMessage(ws, pack, &message); err != nil {
				if message.Method != nil {
					self.Errorf("\t%v\t%v\t%v\t%v\t%v", ws.Request().URL.Path, ws.Request().RemoteAddr, message.Type, message.Method.Name, err)
				} else if message.Object != nil {
					self.Errorf("\t%v\t%v\t%v\t%v\t%v", ws.Request().URL.Path, ws.Request().RemoteAddr, message.Type, message.Object.URI, err)
				} else {
					self.Errorf("\t%v\t%v\t%+v\t%v", ws.Request().URL.Path, ws.Request().RemoteAddr, message, err)
				}
			}
			if message.Method != nil {
				self.Debugf("\t%v\t%v\t%v\t%v\t%v <-", ws.Request().URL.Path, ws.Request().RemoteAddr, message.Type, message.Method.Name, time.Now().Sub(start))
			}
			if message.Object != nil {
				self.Debugf("\t%v\t%v\t%v\t%v\t%v <-", ws.Request().URL.Path, ws.Request().RemoteAddr, message.Type, message.Object.URI, time.Now().Sub(start))
			}
			if self.LogLevel > Trace {
				if message.Method != nil && message.Method.Data != nil {
					self.Tracef("%+v", common.Prettify(message.Method.Data))
				}
				if message.Object != nil && message.Object.Data != nil {
					self.Tracef("%+v", common.Prettify(message.Object.Data))
				}
			}
		} else if err == io.EOF {
			break
		} else {
			self.Errorf("%v", err)
		}
	}
}

func (self *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	websocket.Handler(self.handleConnection).ServeHTTP(w, r)
}
