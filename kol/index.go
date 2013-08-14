package kol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

const (
	index          = "index"
	secondaryIndex = "2i"
	foreignIndex   = "fi"
)

var fkPattern = regexp.MustCompile("fk<([^>]+)>")

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

func foreignKey(
	id []byte,
	idPart []byte,
	typ reflect.Type,
	foreignFieldName,
	idFieldName string,
	foreignFieldType reflect.Type,
	foreignFieldValue reflect.Value,
) (keys [][]byte, err error) {
	var foreignPart []byte
	if foreignPart, err = indexBytes(foreignFieldType, foreignFieldValue); err != nil {
		return
	}
	keys = [][]byte{
		[]byte(foreignIndex),
		[]byte(typ.Name()),
		[]byte(foreignFieldName),
		[]byte(idFieldName),
		foreignPart,
		idPart,
		id,
	}
	return
}

func indexKeys(id []byte, value reflect.Value, typ reflect.Type) (indexed [][][]byte, err error) {
	alreadyIndexed := make(map[string]bool)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if kolTag := field.Tag.Get(kol); kolTag != "" {
			for _, param := range strings.Split(kolTag, ",") {
				if param == index {
					// kol:"index"
					if !alreadyIndexed[field.Name] {
						// Not already indexed
						var keys [][]byte
						// Build an index key
						keys, err = indexKey(id, typ, field.Name, field.Type, value.Field(i))
						if err != nil {
							return
						}
						indexed = append(indexed, keys)
						alreadyIndexed[field.Name] = true
					}
				} else if match := fkPattern.FindStringSubmatch(param); match != nil {
					// Wants it treated as foreign key
					if field.Type.Kind() == reflect.Slice && field.Type.Elem().Kind() == reflect.Uint8 {
						// Is a []byte
						if matchField := value.FieldByName(match[1]); matchField.IsValid() {
							// And the match field exists
							matchFieldType := matchField.Type()
							var keys [][]byte
							// Build a foreign key
							keys, err = foreignKey(id, value.Field(i).Bytes(), typ, match[1], field.Name, matchFieldType, matchField)
							if err != nil {
								return
							}
							indexed = append(indexed, keys)
							if !alreadyIndexed[match[1]] {
								// The match key is not already indexed, build an index key
								keys, err = indexKey(id, typ, match[1], matchFieldType, matchField)
								if err != nil {
									return
								}
								indexed = append(indexed, keys)
								alreadyIndexed[match[1]] = true
							}
						} else {
							err = fmt.Errorf("%v.%v is tagged as %v, but there is no field named %v", typ.Name, field.Name, kolTag, match[1])
							return
						}
					} else {
						err = fmt.Errorf("%v.%v is  tagged as %v, but it is not a []byte", typ.Name, field.Name, kolTag)
						return
					}
				}
			}
		}
	}
	return
}

func (self *DB) index(id []byte, value reflect.Value, typ reflect.Type) (err error) {
	var indexed [][][]byte
	if indexed, err = indexKeys(id, value, typ); err != nil {
		return
	}
	for _, keys := range indexed {
		if err = self.db.Set(keys, []byte{0}); err != nil {
			return
		}
	}
	return
}

func (self *DB) deIndex(id []byte, value reflect.Value, typ reflect.Type) (err error) {
	var indexed [][][]byte
	if indexed, err = indexKeys(id, value, typ); err != nil {
		return
	}
	for _, keys := range indexed {
		if err = self.db.Remove(keys); err != nil {
			return
		}
	}
	return
}
