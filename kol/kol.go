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
	primaryKey = "primaryKey"
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

/*
Del will delete the object with id in the database.
*/
func (self *DB) Del(obj interface{}) error {
	_, id, err := identify(obj)
	if err != nil {
		return err
	}
	if err := self.db.Remove(kc.Keyify(primaryKey, id.String())); err != nil {
		if err.Error() == "no record" {
			err = NotFound
		}
		return err
	}
	return nil
}

/*
Get will find the object with id in the database, and
JSON decode it into result.
*/
func (self *DB) Get(id string, result interface{}) error {
	b, err := self.db.Get(kc.Keyify(primaryKey, id))
	if err != nil {
		if err.Error() == "no record" {
			err = NotFound
		}
		return err
	}
	return json.Unmarshal(b, result)
}

func (self *DB) save(id string, obj interface{}) error {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return self.db.Set(kc.Keyify(primaryKey, id), bytes)
}

func (self *DB) begin() error {
	return self.db.BeginTran(false)
}

func (self *DB) abort() error {
	return self.db.EndTran(false)
}

func (self *DB) commit() error {
	return self.db.EndTran(true)
}

func (self *DB) index(id string, obj interface{}) error {
	fmt.Println("implement index")
	return nil
}

func (self *DB) deIndex(id string, obj interface{}) error {
	fmt.Println("implement deIndex")
	return nil
}

func (self *DB) create(id string, obj interface{}) error {
	if err := self.begin(); err != nil {
		return err
	}
	if err := self.index(id, obj); err != nil {
		return err
	}
	if err := self.save(id, obj); err != nil {
		self.abort()
		return err
	}
	return self.commit()
}

func (self *DB) update(id string, old, obj interface{}) error {
	if err := self.begin(); err != nil {
		return err
	}
	if err := self.deIndex(id, old); err != nil {
		self.abort()
		return err
	}
	if err := self.index(id, obj); err != nil {
		self.abort()
		return err
	}
	if err := self.save(id, obj); err != nil {
		self.abort()
		return err
	}
	return self.commit()
}

/*
Set will JSON encode obj and insert it into the database

Obj must be a pointer to a struct having an Id field that is a string.

If the Id field is empty, a random Id will be provided.

If no object with the same Id exists in the database, a create will be performed,
otherwise an update.
*/
func (self *DB) Set(obj interface{}) error {
	value, id, err := identify(obj)
	if err != nil {
		return err
	}
	if id.String() == "" {
		idString := randomString()
		id.SetString(idString)
		return self.create(idString, obj)
	} else {
		idString := id.String()
		old := reflect.New(value.Type()).Interface()
		if err := self.Get(idString, old); err == nil {
			return self.update(idString, old, obj)
		} else {
			if err != NotFound {
				return err
			}
			return self.create(idString, obj)
		}
	}
}
