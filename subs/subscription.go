package subs

import (
	"code.google.com/p/go.net/websocket"
	"fmt"
	"github.com/zond/kcwraps/kol"
	"reflect"
	"sync"
)

const (
	FetchType = "Fetch"
)

/*
Object is used to send JSON messages to subscribing WebSockets.
*/
type Object struct {
	URL  string
	Data interface{}
}

/*
Message wraps Objects in JSON messages.
*/
type Message struct {
	Type   string
	Object Object
}

/*
Subscription encapsulates a subscription by a WebSocket on an object or a query.
*/
type Subscription struct {
	pack  *Pack
	url   string
	name  string
	Query *kol.Query
	/*
		Call defaults to Subscription.Send, and is used to deliver all data for this Subscription.

		Replace it if you want to filter or decorate the data before sending it with Subscription.Send.
	*/
	Call func(o interface{}, op string)
}

/*
Send will send a message through the WebSocket of this Subscription.

Message.Type will be op, Message.Object.URL will be the url of this subscription and Message.Object.Data will be the JSON representation of i.
*/
func (self *Subscription) Send(i interface{}, op string) {
	if err := websocket.JSON.Send(self.pack.ws, Message{
		Type: op,
		Object: Object{
			Data: i,
			URL:  self.url,
		},
	}); err != nil {
		self.pack.unsubscribeName(self.name)
	}
}

/*
DB returns the DB of the Pack that created this Subscription.
*/
func (self *Subscription) DB() *kol.DB {
	return self.pack.db
}

/*
Subscribe will start this Subscription.

If the Subscription has a Query, the results of the Query will be sent through the WebSocket, and then a subscription for this query will start that continues
sending updates on the results of the query through the WebSocket.

If the Subscription doesn't have a Query, the object will be loaded from the database and sent through the websocket, and then a subscription for that object will start that
continues sending updates on the object through the WebSocket.
*/
func (self *Subscription) Subscribe(object interface{}) {
	if self.Query == nil {
		if err := self.pack.db.Subscribe(self.name, object, kol.AllOps, func(i interface{}, op kol.Operation) {
			self.Call(i, op.String())
		}); err != nil {
			panic(err)
		}
		if err := self.pack.db.Get(object); err != nil {
			if err != kol.NotFound {
				panic(err)
			}
		} else {
			self.Call(object, FetchType)
		}
	} else {
		if err := self.Query.Subscribe(self.name, object, kol.AllOps, func(i interface{}, op kol.Operation) {
			slice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(object)), 1, 1)
			slice.Index(0).Set(reflect.ValueOf(i))
			self.Call(slice.Interface(), op.String())
		}); err != nil {
			panic(err)
		}
		slice := reflect.New(reflect.SliceOf(reflect.TypeOf(object))).Interface()
		if err := self.Query.All(slice); err != nil {
			panic(err)
		} else {
			self.Call(reflect.ValueOf(slice).Elem().Interface(), FetchType)
		}
	}
}

/*
Pack encapsulates a set of Subscriptions from a single WebSocket connected to a single DB.

Use it to unsubscribe all Subscriptions when the WebSocket disconnects.
*/
type Pack struct {
	db   *kol.DB
	ws   *websocket.Conn
	lock *sync.Mutex
	subs map[string]*Subscription
}

/*
New will return a new Pack for db and ws.
*/
func New(db *kol.DB, ws *websocket.Conn) *Pack {
	return &Pack{
		lock: new(sync.Mutex),
		subs: make(map[string]*Subscription),
		ws:   ws,
		db:   db,
	}
}

func (self *Pack) generateName(url string) string {
	return fmt.Sprintf("%v/%v", self.ws.Request().RemoteAddr, url)
}

func (self *Pack) unsubscribeName(name string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	if _, found := self.subs[name]; found {
		self.db.Unsubscribe(name)
		delete(self.subs, name)
	}
}

/*
Unsubscribe will unsubscribe the Subscription for url.
*/
func (self *Pack) Unsubscribe(url string) {
	self.unsubscribeName(self.generateName(url))
}

/*
UnsubscribeAll will unsubscribe all Subscriptions of this Pack.
*/
func (self *Pack) UnsubscribeAll() {
	self.lock.Lock()
	defer self.lock.Unlock()
	for name, _ := range self.subs {
		self.db.Unsubscribe(name)
	}
	self.subs = make(map[string]*Subscription)
}

/*
New will return a new Subscription using the WebSocket and database of this Pack, bound to url.

The new Subscription will have Call set to its Send func.
*/
func (self *Pack) New(url string) (result *Subscription) {
	result = &Subscription{
		pack: self,
		url:  url,
		name: self.generateName(url),
	}
	result.Call = result.Send
	return
}
