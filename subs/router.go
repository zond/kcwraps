package subs

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"time"

	"code.google.com/p/go.net/websocket"
	"github.com/zond/diplicity/common"
	"github.com/zond/kcwraps/kol"
)

const (
	SubscribeType   = "Subscribe"
	UnsubscribeType = "Unsubscribe"
	UpdateType      = "Update"
	CreateType      = "Create"
	DeleteType      = "Delete"
	RPCType         = "RPC"
)

const (
	Fatal = iota
	Error
	Info
	Debug
	Trace
)

type Logger interface {
	Fatalf(format string, params ...interface{})
	Errorf(format string, params ...interface{})
	Infof(format string, params ...interface{})
	Debugf(format string, params ...interface{})
	Tracef(format string, params ...interface{})
}

type Context interface {
	Logger
	Conn() *websocket.Conn
	Pack() *Pack
	Message() *Message
	Principal() string
	Match() []string
	Data() JSON
	Router() *Router
}

type ResourceHandler func(c Context) error

type Resource struct {
	Path     *regexp.Regexp
	Handlers map[string]ResourceHandler
}

func (self *Resource) Handle(op string, handler ResourceHandler) *Resource {
	self.Handlers[op] = handler
	return self
}

type Resources []*Resource

type RPCHandler func(c Context) (result interface{}, err error)

type RPC struct {
	Method  string
	Handler RPCHandler
}

type RPCs []*RPC

func NewRouter(db *kol.DB) (result *Router) {
	result = &Router{
		DB:     db,
		Logger: log.New(os.Stdout, "", 0),
	}
	result.OnUnsubscribeFactory = result.DefaultOnUnsubscribeFactory
	result.EventLoggerFactory = result.DefaultEventLoggerFactory
	result.OnDisconnectFactory = result.DefaultOnDisconnectFactory
	result.OnConnect = result.DefaultOnConnect
	return
}

type Router struct {
	Resources            Resources
	RPCs                 RPCs
	DB                   *kol.DB
	Logger               *log.Logger
	LogLevel             int
	OnUnsubscribeFactory func(ws *websocket.Conn, principal string) func(s *Subscription, reason interface{})
	EventLoggerFactory   func(ws *websocket.Conn, principal string) func(name string, i interface{}, op string, dur time.Duration)
	OnDisconnectFactory  func(ws *websocket.Conn, principal string) func()
	OnConnect            func(ws *websocket.Conn, principal string)
}

func (self *Router) DefaultOnConnect(ws *websocket.Conn, principal string) {
	self.Infof("\t%v\t%v\t%v <-", ws.Request().URL, ws.Request().RemoteAddr, principal)
}

func (self *Router) DefaultOnUnsubscribeFactory(ws *websocket.Conn, principal string) func(s *Subscription, reason interface{}) {
	return func(s *Subscription, reason interface{}) {
		self.Debugf("\t%v\t%v\t%v\t%v\t%v\t[unsubscribing]", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, s.URI(), reason)
		if self.LogLevel > Trace {
			self.Tracef("%s", debug.Stack())
		}
	}
}

func (self *Router) DefaultEventLoggerFactory(ws *websocket.Conn, principal string) func(name string, i interface{}, op string, dur time.Duration) {
	return func(name string, i interface{}, op string, dur time.Duration) {
		self.Debugf("\t%v\t%v\t%v\t%v\t%v\t%v ->", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, op, name, dur)
	}
}

func (self *Router) DefaultOnDisconnectFactory(ws *websocket.Conn, principal string) func() {
	return func() {
		self.Infof("\t%v\t%v\t%v -> [unsubscribing all]", ws.Request().URL.Path, ws.Request().RemoteAddr, principal)
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

func (self *Router) Resource(exp string) (result *Resource) {
	result = &Resource{
		Path: regexp.MustCompile(exp),
	}
	self.Resources = append(self.Resources, result)
	return
}

func (self *Router) RPC(method string, handler RPCHandler) *Router {
	self.RPCs = append(self.RPCs, &RPC{
		Method:  method,
		Handler: handler,
	})
	return self
}

func (self *Router) handleMessage(ws *websocket.Conn, pack *Pack, message *Message, principal string) (err error) {
	c := &defaultContext{
		conn:      ws,
		pack:      pack,
		message:   message,
		principal: principal,
	}
	switch message.Type {
	case UnsubscribeType:
		pack.Unsubscribe(message.Object.URI)
		return
	case SubscribeType, CreateType, UpdateType, DeleteType:
		for _, resource := range self.Resources {
			if match := resource.Path.FindStringSubmatch(message.Object.URI); match != nil {
				if handler, found := resource.Handlers[message.Type]; found {
					c.match = match
					c.data = JSON{message.Object.Data}
					return handler(c)
				}
			}
		}
		return fmt.Errorf("Unrecognized URI for %+v", message)
	case RPCType:
		for _, rpc := range self.RPCs {
			if rpc.Method == message.Method.Name {
				var resp interface{}
				c.data = JSON{message.Method.Data}
				if resp, err = rpc.Handler(c); err != nil {
					return
				}
				return websocket.JSON.Send(ws, Message{
					Type: common.RPCType,
					Method: &Method{
						Name: message.Method.Name,
						Id:   message.Method.Id,
						Data: resp,
					},
				})
			}
		}
		return fmt.Errorf("Unrecognized Method for %+v", message)
	}
	return fmt.Errorf("Unknown message type for %+v", message)
}

type ErrorMessage struct {
	Cause interface{}
	Err   error
}

func (self *Router) DeliverError(ws *websocket.Conn, cause interface{}, err error) {
	if err = websocket.JSON.Send(ws, ErrorMessage{
		Cause: cause,
		Err:   err,
	}); err != nil {
		self.Errorf("%v", err)
	}
}

func (self *Router) handleConnection(ws *websocket.Conn) {
	principal := ""
	if tok := ws.Request().URL.Query().Get("token"); tok != "" {
		token, err := DecodeToken(ws.Request().URL.Query().Get("token"))
		if err != nil {
			self.Errorf("\t%v\t%v\t[invalid token: %v]", ws.Request().URL, ws.Request().RemoteAddr, err)
			self.DeliverError(ws, nil, err)
			return
		}
		principal = token.Principal
	}

	self.Infof("\t%v\t%v\t%v <-", ws.Request().URL, ws.Request().RemoteAddr, principal)
	defer self.OnDisconnectFactory(ws, principal)()

	pack := NewPack(self.DB, ws).OnUnsubscribe(self.OnUnsubscribeFactory(ws, principal)).Logger(self.EventLoggerFactory(ws, principal))
	defer pack.UnsubscribeAll()

	var start time.Time
	for {
		message := &Message{}
		if err := websocket.JSON.Receive(ws, message); err == nil {
			start = time.Now()
			if err = self.handleMessage(ws, pack, message, principal); err != nil {
				if message.Method != nil {
					self.Errorf("\t%v\t%v\t%v\t%v\t%v\t%v", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, message.Type, message.Method.Name, err)
				} else if message.Object != nil {
					self.Errorf("\t%v\t%v\t%v\t%v\t%v\t%v", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, message.Type, message.Object.URI, err)
				} else {
					self.Errorf("\t%v\t%v\t%v\t%+v\t%v", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, message, err)
				}
				self.DeliverError(ws, message, err)
			}
			if message.Method != nil {
				self.Debugf("\t%v\t%v\t%v\t%v\t%v\t%v <-", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, message.Type, message.Method.Name, time.Now().Sub(start))
			}
			if message.Object != nil {
				self.Debugf("\t%v\t%v\t%v\t%v\t%v\t%v <-", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, message.Type, message.Object.URI, time.Now().Sub(start))
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
			self.DeliverError(ws, nil, err)
			self.Errorf("%v", err)
		}
	}
}

func (self *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	websocket.Handler(self.handleConnection).ServeHTTP(w, r)
}
