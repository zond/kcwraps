package subs

import (
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"code.google.com/p/go.net/websocket"
	"github.com/zond/kcwraps/kol"
	"github.com/zond/wsubs/gosubs"
)

/*
Router controls incoming WebSocket messages
*/
type Router struct {
	*gosubs.Router
	DB                   *kol.DB
	OnUnsubscribeFactory func(ws *websocket.Conn, principal string) func(s *Subscription, reason interface{})
	EventLoggerFactory   func(ws *websocket.Conn, principal string) func(name string, i interface{}, op string, dur time.Duration)
}

/*
NewRouter returns a router connected to db
*/
func NewRouter(db *kol.DB) (result *Router) {
	result = &Router{
		Router: gosubs.NewRouter(),
		DB:     db,
	}
	result.OnUnsubscribeFactory = result.DefaultOnUnsubscribeFactory
	result.EventLoggerFactory = result.DefaultEventLoggerFactory
	return
}

/*
DefaultonUnsubscribeFactory will return functions that just log the unsubscribing uri
*/
func (self *Router) DefaultOnUnsubscribeFactory(ws *websocket.Conn, principal string) func(s *Subscription, reason interface{}) {
	return func(s *Subscription, reason interface{}) {
		self.Debugf("%v\t%v\t%v\t%v\t%v\t[unsubscribing]", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, s.Name(), reason)
		if self.LogLevel > gosubs.TraceLevel {
			self.Tracef("%s", debug.Stack())
		}
	}
}

/*
DefaultEventLoggerFactory will return functions that just log the bubbling event
*/
func (self *Router) DefaultEventLoggerFactory(ws *websocket.Conn, principal string) func(name string, i interface{}, op string, dur time.Duration) {
	return func(name string, i interface{}, op string, dur time.Duration) {
		self.Debugf("%v\t%v\t%v\t%v\t%v\t%v ->", ws.Request().URL.Path, ws.Request().RemoteAddr, principal, op, name, dur)
	}
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
	*gosubs.Resource
}

/*
Handle tells the router how to handle a given operation on the resource
*/
func (self *Resource) Handle(op string, handler ResourceHandler) *Resource {
	self.Resource.Handle(op, func(gosubc gosubs.Context) error {
		c, ok := gosubc.(Context)
		if !ok {
			return fmt.Errorf("%+v is not a subs.Context", gosubc)
		}
		return handler(c)
	})
	return self
}

/*
Auth tells the router that the op/handler combination defined
in the last Handle call should only receive messages from authenticated
requests (where the Context has a Principal())
*/
func (self *Resource) Auth() *Resource {
	self.Resource.Auth()
	return self
}

/*
Resource creates a resource receiving messages matching the provided regexp
*/
func (self *Router) Resource(exp string) (result *Resource) {
	return &Resource{
		self.Router.Resource(exp),
	}
}

/*
RPCHandlers handle RPC requests
*/
type RPCHandler func(c Context) (result interface{}, err error)

/*
RPC creates an RPC method receiving messages matching the provided method name
*/
func (self *Router) RPC(method string, handler RPCHandler) (result *gosubs.RPC) {
	return self.Router.RPC(method, func(gosubc gosubs.Context) (result interface{}, err error) {
		c, ok := gosubc.(Context)
		if !ok {
			err = fmt.Errorf("%+v is not a subs.Context", gosubc)
			return
		}
		return handler(c)
	})
}

func (self *Router) handleMessage(ws *websocket.Conn, pack *Pack, message *gosubs.Message, principal string) (err error) {
	c := NewContext(gosubs.NewContext(ws, message, principal, self), pack, self, self.DB)
	switch message.Type {
	case gosubs.UnsubscribeType:
		pack.Unsubscribe(message.Object.URI)
		self.RemoveSubscriber(c.Principal(), message.Object.URI)
		return
	case gosubs.SubscribeType, gosubs.CreateType, gosubs.UpdateType, gosubs.DeleteType:
		return self.HandleResourceMessage(c)
	case gosubs.RPCType:
		return self.HandleRPCMessage(c)
	}
	return fmt.Errorf("Unknown message type for %v", gosubs.Prettify(message))
}

func (self *Router) handleConnection(ws *websocket.Conn) {
	if principal, ok := self.SetupConnection(ws); ok {
		defer self.OnDisconnectFactory(ws, principal)()

		pack := NewPack(self.DB, ws).OnUnsubscribe(self.OnUnsubscribeFactory(ws, principal)).Logger(self.EventLoggerFactory(ws, principal))
		defer pack.UnsubscribeAll()

		handlerFunc := func(message *gosubs.Message) error {
			return self.handleMessage(ws, pack, message, principal)
		}

		self.ProcessMessages(ws, principal, handlerFunc)
	}
}

/*
Implements http.Handler
*/
func (self *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	websocket.Handler(self.handleConnection).ServeHTTP(w, r)
}
