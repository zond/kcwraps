package subs

import (
	"code.google.com/p/go.net/websocket"
	"fmt"
	"github.com/zond/kcwraps/kol"
	"sync"
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
	pack *Pack
	url  string
	name string
}

func (self *Subscription) Name() string {
	return self.name
}

func (self *Subscription) Call(i interface{}, op string) {
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

func (self *Pack) New(url string) *Subscription {
	return &Subscription{
		pack: self,
		url:  url,
		name: self.generateName(url),
	}
}
