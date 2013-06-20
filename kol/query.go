package kol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/zond/kcwraps/kc"
	"github.com/zond/setop"
	"reflect"
)

type qFilter interface {
	source(typ reflect.Type) (result setop.SetOpSource, err error)
	match(typ reflect.Type, value reflect.Value) (result bool, err error)
}

type Or []qFilter

func (self Or) source(typ reflect.Type) (result setop.SetOpSource, err error) {
	op := setop.SetOp{
		Merge: setop.First,
		Type:  setop.Union,
	}
	for _, filter := range self {
		var newSource setop.SetOpSource
		if newSource, err = filter.source(typ); err != nil {
			return
		}
		op.Sources = append(op.Sources, newSource)
	}
	result.SetOp = &op
	return
}

func (self Or) match(typ reflect.Type, value reflect.Value) (result bool, err error) {
	for _, filter := range self {
		if result, err = filter.match(typ, value); err != nil || result {
			return
		}
	}
	return
}

type And []qFilter

func (self And) source(typ reflect.Type) (result setop.SetOpSource, err error) {
	op := setop.SetOp{
		Merge: setop.First,
		Type:  setop.Intersection,
	}
	for _, filter := range self {
		var newSource setop.SetOpSource
		if newSource, err = filter.source(typ); err != nil {
			return
		}
		op.Sources = append(op.Sources, newSource)
	}
	result.SetOp = &op
	return
}

func (self And) match(typ reflect.Type, value reflect.Value) (result bool, err error) {
	result, err = true, nil
	for _, filter := range self {
		if result, err = filter.match(typ, value); err != nil || !result {
			return
		}
	}
	return
}

type Equals struct {
	Field string
	Value interface{}
}

func (self Equals) source(typ reflect.Type) (result setop.SetOpSource, err error) {
	value := reflect.ValueOf(self.Value)
	var b []byte
	if b, err = indexBytes(value.Type(), value); err != nil {
		return
	}
	result = setop.SetOpSource{
		Key: kc.JoinKeys([][]byte{[]byte(secondaryIndex), []byte(typ.Name()), []byte(self.Field), b}),
	}
	return
}

func (self Equals) match(typ reflect.Type, value reflect.Value) (result bool, err error) {
	selfValue := reflect.ValueOf(self.Value)
	var selfBytes []byte
	if selfBytes, err = indexBytes(typ, selfValue); err != nil {
		return
	}
	var otherBytes []byte
	if otherBytes, err = indexBytes(typ, value); err != nil {
		return
	}
	result = bytes.Compare(selfBytes, otherBytes) == 0
	return
}

type Query struct {
	db           *DB
	typ          reflect.Type
	intersection qFilter
	difference   qFilter
}

func (self *Query) Subscribe(name string, obj interface{}, ops Operation, subscriber Subscriber) (err error) {
	var value reflect.Value
	if value, _, err = identify(obj); err != nil {
		return
	}
	self.typ = value.Type()
	self.db.subscriptionsMutex.Lock()
	defer self.db.subscriptionsMutex.Unlock()
	self.db.subscriptions[name] = subscription{
		matcher:    self.match,
		subscriber: subscriber,
		ops:        ops,
		typ:        self.typ,
	}
	return
}

func (self *Query) match(typ reflect.Type, value reflect.Value) (result bool, err error) {
	if self.typ.Name() != typ.Name() {
		return
	}
	if self.intersection != nil {
		if result, err = self.intersection.match(typ, value); err != nil || !result {
			return
		}
	}
	if self.difference != nil {
		if result, err = self.difference.match(typ, value); err != nil || result {
			result = true
			return
		}
	}
	result = true
	return
}

func (self *Query) each(f func(elementPointer reflect.Value) bool) error {
	op := &setop.SetOp{
		Sources: []setop.SetOpSource{
			setop.SetOpSource{
				Key: kc.JoinKeys([][]byte{[]byte(primaryKey), []byte(self.typ.Name())}),
			},
		},
		Type:  setop.Intersection,
		Merge: setop.First,
	}
	if self.intersection != nil {
		source, err := self.intersection.source(self.typ)
		if err != nil {
			return err
		}
		op.Sources = append(op.Sources, source)
	}
	if self.difference != nil {
		source, err := self.difference.source(self.typ)
		if err != nil {
			return err
		}
		op = &setop.SetOp{
			Sources: []setop.SetOpSource{
				setop.SetOpSource{
					SetOp: op,
				},
				source,
			},
			Type:  setop.Difference,
			Merge: setop.First,
		}
	}
	for _, kv := range self.db.db.SetOp(&setop.SetExpression{
		Op: op,
	}) {
		obj := reflect.New(self.typ).Interface()
		if err := json.Unmarshal(kv.Value, obj); err == nil {
			if f(reflect.ValueOf(obj)) {
				break
			}
		}
	}
	return nil
}

func (self *Query) Except(f qFilter) *Query {
	self.difference = f
	return self
}

func (self *Query) Filter(f qFilter) *Query {
	self.intersection = f
	return self
}

func (self *Query) First(result interface{}) (err error) {
	var value reflect.Value
	if value, _, err = identify(result); err != nil {
		return
	}
	self.typ = value.Type()
	err = self.each(func(elementPointer reflect.Value) bool {
		value.Set(elementPointer.Elem())
		return true
	})
	return
}

func (self *Query) All(result interface{}) (err error) {
	slicePtrValue := reflect.ValueOf(result)
	if slicePtrValue.Kind() != reflect.Ptr {
		err = fmt.Errorf("%v is not a pointer", result)
		return
	}
	sliceValue := slicePtrValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		err = fmt.Errorf("%v is not a pointer to a slice", result)
		return
	}
	sliceType := sliceValue.Type()
	sliceElemType := sliceType.Elem()
	pointerSlice := false
	if sliceElemType.Kind() == reflect.Ptr {
		pointerSlice = true
		sliceElemType = sliceElemType.Elem()
	}
	if sliceElemType.Kind() != reflect.Struct {
		err = fmt.Errorf("%v is not pointer to a slice of structs or structpointers", result)
		return
	}
	self.typ = sliceElemType
	err = self.each(func(elementPointer reflect.Value) bool {
		if pointerSlice {
			sliceValue.Set(reflect.Append(sliceValue, elementPointer))
		} else {
			sliceValue.Set(reflect.Append(sliceValue, elementPointer.Elem()))
		}
		return false
	})
	return
}
