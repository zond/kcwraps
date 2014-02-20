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
	"github.com/zond/kcwraps/kol"
)

const (
	SubscribeType   = "Subscribe"
	UnsubscribeType = "Unsubscribe"
	UpdateType      = "Update"
	CreateType      = "Create"
	DeleteType      = "Delete"
	RPCType         = "RPC"
	ErrorType       = "Error"
)

const (
	FatalLevel = iota
	ErrorLevel
	InfoLevel
	DebugLevel
	TraceLevel
)

/*
Loggers be loggin'
*/
type Logger interface {
	Fatalf(format string, params ...interface{})
	Errorf(format string, params ...interface{})
	Infof(format string, params ...interface{})
	Debugf(format string, params ...interface{})
	Tracef(format string, params ...interface{})
}

/*
Context describes a single WebSocket message and its environment
*/
type Context interface {
	Logger
	DB() *kol.DB
	Conn() *websocket.Conn
	Pack() *Pack
	Message() *Message
	Principal() string
	Match() []string
	Data() JSON
	Router() *Router
}

/*
ResourceHandler will handle a message regarding an operation on a resource
*/
type ResourceHandler func(c Context) error

/*
Resource describes how the router ought to treat incoming requests for a
resource found under a given URI regexp
*/
type Resource struct {
	Path          *regexp.Regexp
	Handlers      map[string]ResourceHandler
	Authenticated map[string]bool
	lastOp        string
}

/*
Handle tells the router how to handle a given operation on the resource
*/
func (self *Resource) Handle(op string, handler ResourceHandler) *Resource {
	self.Handlers[op] = handler
	self.lastOp = op
	return self
}

/*
Auth tells the router that the op/handler combination defined
in the last Handle call should only receive messages from authenticated
requests (where the Context has a Principal())
*/
func (self *Resource) Auth() *Resource {
	self.Authenticated[self.lastOp] = true
	return self
}

type Resources []*Resource

/*
RPCHandlers handle RPC requests
*/
type RPCHandler func(c Context) (result interface{}, err error)

/*
RPC describes how the router ought to treat incoming requests for
a given RPC method
*/
type RPC struct {
	Method        string
	Handler       RPCHandler
	Authenticated bool
}

/*
Auth tells the router that the RPC should only receive messages from
authenticated requests (where the Context has a Principal())
*/
func (self *RPC) Auth() *RPC {
	self.Authenticated = true
	return self
}

type RPCs []*RPC

/*
NewRouter returns a router connected to db
*/
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

/*
Router controls incoming WebSocket messages
*/
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

/*
DefaultOnConnect will just log the incoming connection
*/
func (self *Router) DefaultOnConnect(ws *websocket.Conn, principal string) {
	self.Infof("\t%v\t%v\t%v <-", ws.Request().URL, ws.Request().RemoteAddr, principal)
}

/*
DefaultonUnsubscribeFactory will return functions that just log the unsubscribing connection
*/
func (self *Router) DefaultOnUnsubscribeFactory(ws *websocket.Conn, principal string) func(s *Subscription, reason interface{}) {
	return func(s *Subscription, reason interface{}) {
		self.Debugf("\t%v\t%v\t%v\t%v\t%v\t[unsubscribing]", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, s.URI(), reason)
		if self.LogLevel > TraceLevel {
			self.Tracef("%s", debug.Stack())
		}
	}
}

/*
DefaultEventLoggerFactory will return functions that just log the bubbling event
*/
func (self *Router) DefaultEventLoggerFactory(ws *websocket.Conn, principal string) func(name string, i interface{}, op string, dur time.Duration) {
	return func(name string, i interface{}, op string, dur time.Duration) {
		self.Debugf("\t%v\t%v\t%v\t%v\t%v\t%v ->", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, op, name, dur)
	}
}

/*
DefaultOnDisconnectFactory will return functions that just log the disconnecting connection
*/
func (self *Router) DefaultOnDisconnectFactory(ws *websocket.Conn, principal string) func() {
	return func() {
		self.Infof("\t%v\t%v\t%v -> [unsubscribing all]", ws.Request().URL.Path, ws.Request().RemoteAddr, principal)
	}
}

/*
Logf will log the format and args if level is less than the LogLevel of this Router
*/
func (self *Router) Logf(level int, format string, args ...interface{}) {
	if level <= self.LogLevel {
		log.Printf(format, args...)
	}
}

/*
Fatalf is shorthand for Logf(FatalLevel...
*/
func (self *Router) Fatalf(format string, args ...interface{}) {
	self.Logf(FatalLevel, "\033[1;31mFATAL\t"+format+"\033[0m", args...)
}

/*
Errorf is shorthand for Logf(ErrorLevel...
*/
func (self *Router) Errorf(format string, args ...interface{}) {
	self.Logf(ErrorLevel, "\033[31mERROR\t"+format+"\033[0m", args...)
}

/*
Infof is shorthand for Logf(InfoLevel...
*/
func (self *Router) Infof(format string, args ...interface{}) {
	self.Logf(InfoLevel, "INFO\t"+format, args...)
}

/*
Debugf is shorthand for Logf(DebugLevel...
*/
func (self *Router) Debugf(format string, args ...interface{}) {
	self.Logf(DebugLevel, "\033[32mDEBUG\t"+format+"\033[0m", args...)
}

/*
Tracef is shorthand for Logf(TraceLevel...
*/
func (self *Router) Tracef(format string, args ...interface{}) {
	self.Logf(TraceLevel, "\033[1;32mTRACE\t"+format+"\033[0m", args...)
}

/*
Resource creates a resource receiving messages matching the provided regexp
*/
func (self *Router) Resource(exp string) (result *Resource) {
	result = &Resource{
		Path:          regexp.MustCompile(exp),
		Handlers:      map[string]ResourceHandler{},
		Authenticated: map[string]bool{},
	}
	self.Resources = append(self.Resources, result)
	return
}

/*
RPC creates an RPC method receiving messages matching the provided method name
*/
func (self *Router) RPC(method string, handler RPCHandler) (result *RPC) {
	result = &RPC{
		Method:  method,
		Handler: handler,
	}
	self.RPCs = append(self.RPCs, result)
	return
}

func (self *Router) handleMessage(ws *websocket.Conn, pack *Pack, message *Message, principal string) (err error) {
	c := &defaultContext{
		conn:      ws,
		pack:      pack,
		message:   message,
		principal: principal,
		router:    self,
	}
	switch message.Type {
	case UnsubscribeType:
		pack.Unsubscribe(message.Object.URI)
		return
	case SubscribeType, CreateType, UpdateType, DeleteType:
		for _, resource := range self.Resources {
			if !resource.Authenticated[message.Type] || principal != "" {
				if handler, found := resource.Handlers[message.Type]; found {
					if match := resource.Path.FindStringSubmatch(message.Object.URI); match != nil {
						c.match = match
						c.data = JSON{message.Object.Data}
						return handler(c)
					}
				}
			}
		}
		return fmt.Errorf("Unrecognized URI for %v", Prettify(message))
	case RPCType:
		for _, rpc := range self.RPCs {
			if !rpc.Authenticated || principal != "" {
				if rpc.Method == message.Method.Name {
					var resp interface{}
					c.data = JSON{message.Method.Data}
					if resp, err = rpc.Handler(c); err != nil {
						return
					}
					return websocket.JSON.Send(ws, Message{
						Type: RPCType,
						Method: &Method{
							Name: message.Method.Name,
							Id:   message.Method.Id,
							Data: resp,
						},
					})
				}
			}
		}
		return fmt.Errorf("Unrecognized Method for %v", Prettify(message))
	}
	return fmt.Errorf("Unknown message type for %v", Prettify(message))
}

/*
DeliverError sends an error message along the provided WebSocket connection
*/
func (self *Router) DeliverError(ws *websocket.Conn, cause interface{}, err error) {
	if err = websocket.JSON.Send(ws, &Message{
		Type: ErrorType,
		Error: &Error{
			Cause: cause,
			Error: err,
		},
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
			if self.LogLevel > TraceLevel {
				if message.Method != nil && message.Method.Data != nil {
					self.Tracef("%+v", Prettify(message.Method.Data))
				}
				if message.Object != nil && message.Object.Data != nil {
					self.Tracef("%+v", Prettify(message.Object.Data))
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

/*
Implements http.Handler
*/
func (self *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	websocket.Handler(self.handleConnection).ServeHTTP(w, r)
}
