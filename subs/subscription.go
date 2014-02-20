package subs

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"code.google.com/p/go.net/websocket"
	"github.com/zond/kcwraps/kol"
)

const (
	FetchType = "Fetch"
)

func Prettify(obj interface{}) string {
	b, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(b)
}

/*
JSON wraps anything that is a JSON object.
*/
type JSON struct {
	Data interface{}
}

/*
Get returns the value under key as another JSON.
*/
func (self JSON) Get(key string) JSON {
	return JSON{self.Data.(map[string]interface{})[key]}
}

/*
Overwrite will JSON encode itself and decode it into dest.
*/
func (self JSON) Overwrite(dest interface{}) {
	b, err := json.Marshal(self.Data)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(b, dest)
	if err != nil {
		panic(err)
	}
}

/*
GetStringSLice returns the value under key as a string slice.
*/
func (self JSON) GetStringSlice(key string) (result []string) {
	is := self.Data.(map[string]interface{})[key].([]interface{})
	result = make([]string, len(is))
	for index, i := range is {
		result[index] = i.(string)
	}
	return
}

/*
GetString returns the value under key as a string.
*/
func (self JSON) GetString(key string) string {
	return self.Data.(map[string]interface{})[key].(string)
}

/*
Message wraps Objects in JSON messages.
*/
type Message struct {
	Type   string
	Object *Object `json:",omitempty"`
	Method *Method `json:",omitempty"`
	Error  *Error  `json:",omitempty"`
}

type Error struct {
	Cause interface{}
	Error error
}

/*
Object is used to send JSON messages to subscribing WebSockets.
*/
type Object struct {
	URI  string
	Data interface{} `json:",omitempty"`
}

/*
Method is used to send JSON RPC requests.
*/
type Method struct {
	Name string
	Id   string
	Data interface{} `json:",omitempty"`
}

/*
Subscription encapsulates a subscription by a WebSocket on an object or a query.
*/
type Subscription struct {
	pack  *Pack
	uri   string
	name  string
	Query *kol.Query
	/*
		Call defaults to Subscription.Send, and is used to deliver all data for this Subscription.

		Replace it if you want to filter or decorate the data before sending it with Subscription.Send.
	*/
	Call func(o interface{}, op string) error
	/*
	  OnUnsubscribe will be called if this Subscription gets automatically unsubscribed due to panic or error.
	*/
	UnsubscribeListener func(self *Subscription, reason interface{})
	/*
	  Logger will be called whenever data is sent over the socket.
	*/
	Logger func(o interface{}, op string, dur time.Duration)
}

/*
Name returns the name of the Subscription.
*/
func (self *Subscription) Name() string {
	return self.name
}

/*
URI returns the URI of the subscription.
*/
func (self *Subscription) URI() string {
	return self.uri
}

/*
Send will send a message through the WebSocket of this Subscription.

Message.Type will be op, Message.Object.URI will be the uri of this subscription and Message.Object.Data will be the JSON representation of i.
*/
func (self *Subscription) Send(i interface{}, op string) (err error) {
	return websocket.JSON.Send(self.pack.ws, Message{
		Type: op,
		Object: &Object{
			Data: i,
			URI:  self.uri,
		},
	})
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
func (self *Subscription) Subscribe(object interface{}) error {
	start := time.Now()
	var sub *kol.Subscription
	var err error
	if self.Query == nil {
		if sub, err = self.pack.db.Subscription(self.name, object, kol.AllOps, func(i interface{}, op kol.Operation) error {
			return self.Call(i, op.String())
		}); err != nil {
			return err
		}
	} else {
		if sub, err = self.Query.Subscription(self.name, object, kol.AllOps, func(i interface{}, op kol.Operation) error {
			slice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(object)), 1, 1)
			slice.Index(0).Set(reflect.ValueOf(i))
			return self.Call(slice.Interface(), op.String())
		}); err != nil {
			return err
		}
	}
	if self.UnsubscribeListener != nil {
		sub.UnsubscribeListener = func(name string, reason interface{}) {
			self.UnsubscribeListener(self, reason)
		}
	}
	if self.Logger != nil {
		sub.Logger = func(i interface{}, op kol.Operation, dur time.Duration) {
			self.Logger(i, op.String(), dur)
		}
	}
	sub.Subscribe()
	self.pack.lock.Lock()
	defer self.pack.lock.Unlock()
	self.pack.subs[self.name] = self
	if self.Query == nil {
		if err := self.pack.db.Get(object); err != nil {
			if err != kol.NotFound {
				return err
			} else {
				return nil
			}
		} else {
			if self.Logger != nil {
				defer func() {
					self.Logger(object, FetchType, time.Now().Sub(start))
				}()
			}
			return self.Call(object, FetchType)
		}
	} else {
		slice := reflect.New(reflect.SliceOf(reflect.TypeOf(object))).Interface()
		if err := self.Query.All(slice); err != nil {
			return err
		} else {
			iface := reflect.ValueOf(slice).Elem().Interface()
			if self.Logger != nil {
				defer func() {
					self.Logger(iface, FetchType, time.Now().Sub(start))
				}()
			}
			return self.Call(iface, FetchType)
		}
	}
}

/*
Pack encapsulates a set of Subscriptions from a single WebSocket connected to a single DB.

Use it to unsubscribe all Subscriptions when the WebSocket disconnects.
*/
type Pack struct {
	db                  *kol.DB
	ws                  *websocket.Conn
	lock                *sync.Mutex
	subs                map[string]*Subscription
	unsubscribeListener func(sub *Subscription, reason interface{})
	logger              func(uri string, i interface{}, op string, dur time.Duration)
}

/*
New will return a new Pack for db and ws.
*/
func NewPack(db *kol.DB, ws *websocket.Conn) *Pack {
	return &Pack{
		lock: new(sync.Mutex),
		subs: make(map[string]*Subscription),
		ws:   ws,
		db:   db,
	}
}

func (self *Pack) OnUnsubscribe(f func(sub *Subscription, reason interface{})) *Pack {
	self.unsubscribeListener = f
	return self
}

func (self *Pack) Logger(f func(name string, i interface{}, op string, dur time.Duration)) *Pack {
	self.logger = f
	return self
}

func (self *Pack) generateName(uri string) string {
	return fmt.Sprintf("%v/%v", self.ws.Request().RemoteAddr, uri)
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
Unsubscribe will unsubscribe the Subscription for uri.
*/
func (self *Pack) Unsubscribe(uri string) {
	self.unsubscribeName(self.generateName(uri))
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
New will return a new Subscription using the WebSocket and database of this Pack, bound to uri.

The new Subscription will have Call set to its Send func.
*/
func (self *Pack) New(uri string) (result *Subscription) {
	result = &Subscription{
		pack:                self,
		uri:                 uri,
		name:                self.generateName(uri),
		UnsubscribeListener: self.unsubscribeListener,
		Logger: func(i interface{}, op string, dur time.Duration) {
			self.logger(uri, i, op, dur)
		},
	}
	result.Call = result.Send
	return
}
