package kol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/zond/kcwraps/kc"
	"math/rand"
	"reflect"
	"time"
)

const (
	primaryKey = "pk"
	kol        = "kol"
)

var NotFound = fmt.Errorf("Not found")

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randomString() string {
	b := make([]byte, 24)
	for i, _ := range b {
		b[i] = byte(rand.Int31())
	}
	return base64.StdEncoding.EncodeToString(b)
}

func identify(obj interface{}) (value, id reflect.Value, err error) {
	ptrValue := reflect.ValueOf(obj)
	if ptrValue.Kind() != reflect.Ptr {
		err = fmt.Errorf("%v is not a pointer", obj)
		return
	}
	value = ptrValue.Elem()
	if value.Kind() != reflect.Struct {
		err = fmt.Errorf("%v is not a pointer to a struct", obj)
		return
	}
	id = value.FieldByName("Id")
	if id.Kind() == reflect.Invalid {
		err = fmt.Errorf("%v does not have an Id field", obj)
		return
	}
	if !id.CanSet() {
		err = fmt.Errorf("%v can not assign its Id field", obj)
		return
	}
	if id.Kind() != reflect.String {
		err = fmt.Errorf("%v does not have a string Id field", obj)
	}
	return
}

type DB struct {
	db kc.DB
}

func New(path string) (result *DB, err error) {
	var kcdb *kc.DB
	if kcdb, err = kc.New(path); err != nil {
		return
	}
	result = &DB{
		db: *kcdb,
	}
	return
}

func (self *DB) Clear() error {
	return self.db.Clear()
}

func (self *DB) Close() error {
	return self.db.Close()
}

func (self *DB) trans(f func() error) (err error) {
	if err = self.db.BeginTran(false); err != nil {
		return
	}
	if err = f(); err != nil {
		self.db.EndTran(false)
		return err
	}
	if err = self.db.EndTran(true); err != nil {
		return self.db.EndTran(false)
	}
	return
}

/*
Del will delete the obj from the database.

Obj must be a pointer to a struct having a string Id field.
*/
func (self *DB) Del(obj interface{}) error {
	value, id, err := identify(obj)
	if err != nil {
		return err
	}
	typ := value.Type()
	return self.trans(func() error {
		b, err := self.db.Get(kc.Keyify(primaryKey, typ.Name(), id.String()))
		if err == nil {
			if err := json.Unmarshal(b, obj); err != nil {
				return err
			}
			if err := self.deIndex(id.String(), value, typ); err != nil {
				return err
			}
		} else if err.Error() != "no record" {
			return err
		}
		if err := self.db.Remove(kc.Keyify(primaryKey, typ.Name(), id.String())); err != nil {
			if err.Error() == "no record" {
				err = NotFound
			}
			return err
		}
		return nil
	})
}

/*
Get will find the object with id in the database, and JSON decode it into result.

Result must be a pointer to a struct having a string Id field.
*/
func (self *DB) Get(id string, result interface{}) error {
	value, _, err := identify(result)
	if err != nil {
		return err
	}
	b, err := self.db.Get(kc.Keyify(primaryKey, value.Type().Name(), id))
	if err != nil {
		if err.Error() == "no record" {
			err = NotFound
		}
		return err
	}
	return json.Unmarshal(b, result)
}

func (self *DB) save(id string, typ reflect.Type, obj interface{}) error {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return self.db.Set(kc.Keyify(primaryKey, typ.Name(), id), bytes)
}

func (self *DB) create(id string, value reflect.Value, typ reflect.Type, obj interface{}, inTrans bool) error {
	creator := func() error {
		if err := self.index(id, value, typ); err != nil {
			return err
		}
		return self.save(id, typ, obj)
	}
	if inTrans {
		return creator()
	} else {
		return self.trans(creator)
	}
}

func (self *DB) update(id string, objValue reflect.Value, typ reflect.Type, old, obj interface{}) error {
	if err := self.deIndex(id, reflect.ValueOf(old), typ); err != nil {
		return err
	}
	if err := self.index(id, objValue, typ); err != nil {
		return err
	}
	return self.save(id, typ, obj)
}

/*
Set will JSON encode obj and insert it into the database

Obj must be a pointer to a struct having a string Id field.

If the Id field is empty, a random Id will be set.
*/
func (self *DB) Set(obj interface{}) error {
	value, id, err := identify(obj)
	if err != nil {
		return err
	}
	if idString := id.String(); idString == "" {
		idString = randomString()
		id.SetString(idString)
		return self.create(idString, value, value.Type(), obj, false)
	} else {
		typ := value.Type()
		old := reflect.New(typ).Interface()
		return self.trans(func() error {
			if err := self.Get(idString, old); err == nil {
				return self.update(idString, value, typ, old, obj)
			} else {
				if err != NotFound {
					return err
				}
				return self.create(idString, value, value.Type(), obj, true)
			}
		})
	}
}