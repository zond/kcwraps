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

type Object struct {
	URL  string
	Data interface{}
}

type Message struct {
	Type   string
	Object Object
}

type Subscription struct {
	pack  *Pack
	url   string
	name  string
	Query *kol.Query
	Call  func(o interface{}, op string)
}

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

func (self *Subscription) DB() *kol.DB {
	return self.pack.db
}

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

type Pack struct {
	db   *kol.DB
	ws   *websocket.Conn
	lock *sync.Mutex
	subs map[string]*Subscription
}

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

func (self *Pack) Unsubscribe(url string) {
	self.unsubscribeName(self.generateName(url))
}

func (self *Pack) UnsubscribeAll() {
	self.lock.Lock()
	defer self.lock.Unlock()
	for name, _ := range self.subs {
		self.db.Unsubscribe(name)
	}
	self.subs = make(map[string]*Subscription)
}

func (self *Pack) New(url string) (result *Subscription) {
	result = &Subscription{
		pack: self,
		url:  url,
		name: self.generateName(url),
	}
	result.Call = result.Send
	return
}
