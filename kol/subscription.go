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

// Subscribers get updates when objects are updated.
type Subscriber func(obj interface{}, op Operation)

type matcher func(typ reflect.Type, value reflect.Value) (result bool, err error)

type subscription struct {
	matcher    matcher
	subscriber Subscriber
	ops        Operation
	typ        reflect.Type
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
Subscribe will add a subscription to all updates of a given object in the database.

name is used to separate different subscriptions, and to unsubscribe.

ops is the binary OR of the operations this subscription should follow.
*/
func (self *DB) Subscribe(name string, obj interface{}, ops Operation, subscriber Subscriber) (err error) {
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
	self.subscriptions[name] = subscription{
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
		subscriber: subscriber,
		ops:        ops,
		typ:        wantedType,
	}
	return
}

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
		oldMatch := false
		newMatch := false
		var err error
		if oldValue != nil {
			if oldMatch, err = subscription.matcher(typ, *oldValue); err != nil {
				panic(err)
			}
		}
		if newValue != nil {
			if newMatch, err = subscription.matcher(typ, *newValue); err != nil {
				panic(err)
			}
		}
		if oldMatch && newMatch {
			cpy := reflect.New(typ)
			cpy.Elem().Set(*newValue)
			go subscription.subscriber(cpy.Interface(), Update)
		} else if oldMatch {
			cpy := reflect.New(typ)
			cpy.Elem().Set(*oldValue)
			go subscription.subscriber(cpy.Interface(), Delete)
		} else if newMatch {
			cpy := reflect.New(typ)
			cpy.Elem().Set(*newValue)
			go subscription.subscriber(cpy.Interface(), Create)
		}
	}
}
