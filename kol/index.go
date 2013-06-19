package kol

import (
	"fmt"
	"reflect"
	"strings"
)

const (
	index          = "index"
	secondaryIndex = "2i"
)

func eachIndexedAttribute(value reflect.Value, typ reflect.Type, f func(key, value []byte) error) error {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if kolTag := field.Tag.Get(kol); kolTag != "" {
			isIndexed := false
			for _, param := range strings.Split(kolTag, ",") {
				if param == index {
					isIndexed = true
					break
				}
			}
			if isIndexed {
				var indexValue []byte
				switch field.Type.Kind() {
				case reflect.String:
					indexValue = []byte(value.Field(i).String())
				default:
					return fmt.Errorf("%v.%v is not an indexable type", value, field.Name)
				}
				if err := f([]byte(field.Name), indexValue); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (self *DB) index(id string, value reflect.Value, typ reflect.Type) error {
	eachIndexedAttribute(value, typ, func(key, value []byte) error {
		keys := [][]byte{
			[]byte(secondaryIndex),
			[]byte(typ.Name()),
			key,
			value,
			[]byte(id),
		}
		if err := self.db.Set(keys, []byte{0}); err != nil {
			return err
		}
		return nil
	})
	return nil
}

func (self *DB) deIndex(id string, value reflect.Value, typ reflect.Type) error {
	eachIndexedAttribute(value, typ, func(key, value []byte) error {
		keys := [][]byte{
			[]byte(secondaryIndex),
			[]byte(typ.Name()),
			key,
			value,
			[]byte(id),
		}
		if err := self.db.Remove(keys); err != nil {
			return err
		}
		return nil
	})
	return nil
}
