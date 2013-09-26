package kol

import (
	"bytes"
	"fmt"
	"reflect"
)

type Operation int

func (self Operation) String() string {
	switch self {
	case Create:
		return "Create"
	case Update:
		return "Update"
	case Delete:
		return "Delete"
	}
	panic(fmt.Errorf("Unknown Operation: %v", self))
}

const (
	Create Operation = 1 << iota
	Update
	Delete
)

// AllOps is the binary OR of all the database operations you can subscribe to.
var AllOps = Create | Update | Delete

/*
Subscribers get updates when objects are updated.
If the Subscriber returns an error or panics, it will be unsubscribed.
*/
type Subscriber func(obj interface{}, op Operation) error

/*
UnsubscribeListener is used to notify a user of a Subscriber that the Subscriber
has been unsubscribed.
*/
type UnsubscribeListener func(name string, reason interface{})

type matcher func(typ reflect.Type, value reflect.Value) (result bool, err error)

type subscription struct {
	db                  *DB
	name                string
	matcher             matcher
	subscriber          Subscriber
	unsubscribeListener UnsubscribeListener
	ops                 Operation
	typ                 reflect.Type
}

func (self *subscription) unsubscribe(reason interface{}) {
	self.db.Unsubscribe(self.name)
	if self.unsubscribeListener != nil {
		self.unsubscribeListener(self.name, reason)
	}
}

func (self *subscription) call(obj interface{}, op Operation) {
	if err := self.subscriber(obj, op); err != nil {
		self.unsubscribe(err)
	}
}

func (self *subscription) handle(typ reflect.Type, oldValue, newValue *reflect.Value) {
	defer func() {
		if e := recover(); e != nil {
			self.unsubscribe(e)
			panic(e)
		}
	}()
	var err error
	oldMatch := false
	newMatch := false
	if oldValue != nil {
		if oldMatch, err = self.matcher(typ, *oldValue); err != nil {
			self.unsubscribe(err)
			return
		}
	}
	if newValue != nil {
		if newMatch, err = self.matcher(typ, *newValue); err != nil {
			self.unsubscribe(err)
			return
		}
	}
	if oldMatch && newMatch && self.ops&Update == Update {
		cpy := reflect.New(typ)
		cpy.Elem().Set(*newValue)
		self.call(cpy.Interface(), Update)
	} else if oldMatch && self.ops&Delete == Delete {
		cpy := reflect.New(typ)
		cpy.Elem().Set(*oldValue)
		self.call(cpy.Interface(), Delete)
	} else if newMatch && self.ops&Create == Create {
		cpy := reflect.New(typ)
		cpy.Elem().Set(*newValue)
		self.call(cpy.Interface(), Create)
	}
}

/*
Unsubscribe will remove a subscription.
*/
func (self *DB) Unsubscribe(name string) {
	self.subscriptionsMutex.Lock()
	defer self.subscriptionsMutex.Unlock()
	delete(self.subscriptions, name)
}

/*
Subscribe is a shorthand for SubscribeWithUnsubscribeListener with a nil UnsubscribeListener.
*/
func (self *DB) Subscribe(name string, obj interface{}, ops Operation, subscriber Subscriber) (err error) {
	return self.SubscribeWithUnsubscribeListener(name, obj, ops, subscriber, nil)
}

/*
SubscribeWithUnsubscribeListener will add a subscription to all updates of a given object in the database.

name is used to separate different subscriptions, and to unsubscribe.

ops is the binary OR of the operations this subscription should follow.

subscriber will be called on all updates of objects with the same id.

unsubscribeListener (if non nil) will be called when the subscriber is automatically unsubscribed due to panic or error.
*/
func (self *DB) SubscribeWithUnsubscribeListener(name string, obj interface{}, ops Operation, subscriber Subscriber, unsubscribeListener UnsubscribeListener) (err error) {
	var wantedValue reflect.Value
	var wantedId reflect.Value
	if wantedValue, wantedId, err = identify(obj); err != nil {
		return
	}
	wantedType := wantedValue.Type()
	wantedBytes := make([]byte, len(wantedId.Bytes()))
	copy(wantedBytes, wantedId.Bytes())
	self.subscriptionsMutex.Lock()
	defer self.subscriptionsMutex.Unlock()
	self.subscriptions[name] = &subscription{
		name: name,
		db:   self,
		matcher: func(typ reflect.Type, value reflect.Value) (result bool, err error) {
			if typ.Name() != wantedType.Name() {
				return
			}
			if bytes.Compare(value.FieldByName(idField).Bytes(), wantedBytes) != 0 {
				return
			}
			result = true
			return
		},
		subscriber:          subscriber,
		unsubscribeListener: unsubscribeListener,
		ops:                 ops,
		typ:                 wantedType,
	}
	return
}

/*
EmitUpdate will trigger an Update event on obj.

Useful when chaining events, such as when an update of an inner objects
should cause an updated of an outer object.
*/
func (self *DB) EmitUpdate(obj interface{}) {
	value := reflect.ValueOf(obj).Elem()
	self.emit(reflect.TypeOf(value.Interface()), &value, &value)
}

func (self *DB) emit(typ reflect.Type, oldValue, newValue *reflect.Value) {
	if oldValue != nil && newValue != nil {
		if chain := newValue.Addr().MethodByName("Updated"); chain.IsValid() {
			chain.Call([]reflect.Value{reflect.ValueOf(self), oldValue.Addr()})
		}
	} else if newValue != nil {
		if chain := newValue.Addr().MethodByName("Created"); chain.IsValid() {
			chain.Call([]reflect.Value{reflect.ValueOf(self)})
		}
	} else if oldValue != nil {
		if chain := oldValue.Addr().MethodByName("Deleted"); chain.IsValid() {
			chain.Call([]reflect.Value{reflect.ValueOf(self)})
		}
	}
	self.subscriptionsMutex.RLock()
	defer self.subscriptionsMutex.RUnlock()
	for _, subscription := range self.subscriptions {
		go subscription.handle(typ, oldValue, newValue)
	}
}
