package kol

import (
	"bytes"
	"reflect"
)

type Operation int

const (
	Create Operation = 1 << iota
	Update
	Delete
)

var AllOps = Create | Update | Delete

type Subscriber func(obj interface{}, op Operation)

type matcher func(typ reflect.Type, value reflect.Value) (result bool, err error)

type subscription struct {
	matcher    matcher
	subscriber Subscriber
	ops        Operation
	typ        reflect.Type
}

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

func (self *DB) emit(typ reflect.Type, value reflect.Value, op Operation) {
	self.subscriptionsMutex.RLock()
	defer self.subscriptionsMutex.RUnlock()
	for _, subscription := range self.subscriptions {
		if subscription.ops&op != 0 {
			if result, err := subscription.matcher(typ, value); err != nil {
				panic(err)
			} else if result {
				cpy := reflect.New(typ)
				cpy.Elem().Set(value)
				go subscription.subscriber(cpy.Interface(), op)
			}
		}
	}
}
