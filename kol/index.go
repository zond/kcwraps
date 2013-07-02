package kol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
)

const (
	index          = "index"
	secondaryIndex = "2i"
)

func indexBytes(typ reflect.Type, value reflect.Value) (b []byte, err error) {
	switch typ.Kind() {
	case reflect.String:
		b = []byte(value.String())
	case reflect.Int:
		buf := new(bytes.Buffer)
		if err = binary.Write(buf, binary.BigEndian, value.Int()); err != nil {
			return
		}
		b = buf.Bytes()
	case reflect.Slice:
		switch typ.Elem().Kind() {
		case reflect.Uint8:
			b = value.Bytes()
		default:
			err = fmt.Errorf("%v is not an indexable type", typ)
		}
	case reflect.Bool:
		if value.Bool() {
			b = []byte{1}
		} else {
			b = []byte{0}
		}
	default:
		err = fmt.Errorf("%v is not an indexable type", typ)
	}
	return
}

func indexKey(id []byte, typ reflect.Type, fieldName string, fieldType reflect.Type, fieldValue reflect.Value) (keys [][]byte, err error) {
	var valuePart []byte
	if valuePart, err = indexBytes(fieldType, fieldValue); err != nil {
		return
	}
	keys = [][]byte{
		[]byte(secondaryIndex),
		[]byte(typ.Name()),
		[]byte(fieldName),
		valuePart,
		id,
	}
	return
}

func eachIndexedKey(id []byte, value reflect.Value, typ reflect.Type, f func(keys [][]byte) error) error {
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
				keys, err := indexKey(id, typ, field.Name, field.Type, value.Field(i))
				if err != nil {
					return err
				}
				if err = f(keys); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (self *DB) index(id []byte, value reflect.Value, typ reflect.Type) error {
	return eachIndexedKey(id, value, typ, func(keys [][]byte) error {
		if err := self.db.Set(keys, []byte{0}); err != nil {
			return err
		}
		return nil
	})
}

func (self *DB) deIndex(id []byte, value reflect.Value, typ reflect.Type) error {
	return eachIndexedKey(id, value, typ, func(keys [][]byte) error {
		if err := self.db.Remove(keys); err != nil {
			return err
		}
		return nil
	})
}
