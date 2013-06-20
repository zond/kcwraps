package kol

import (
	"encoding/json"
	"fmt"
	"github.com/zond/kcwraps/kc"
	"github.com/zond/setop"
	"reflect"
)

type qFilter interface {
	source(typ reflect.Type) (result setop.SetOpSource, err error)
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

type Query struct {
	db           *DB
	typ          reflect.Type
	intersection qFilter
	difference   qFilter
}

func (self *Query) each(f func(elementPointer reflect.Value)) error {
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
			f(reflect.ValueOf(obj))
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
	err = self.each(func(elementPointer reflect.Value) {
		if pointerSlice {
			sliceValue.Set(reflect.Append(sliceValue, elementPointer))
		} else {
			sliceValue.Set(reflect.Append(sliceValue, elementPointer.Elem()))
		}
	})
	return
}
