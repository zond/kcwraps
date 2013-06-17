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

func (self *DB) Del(id string) error {
	return self.db.Remove(kc.Keyify(primaryKey, id))
}

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

func (self *DB) create(idField reflect.Value, obj interface{}) error {
	id := randomString()
	idField.SetString(id)
	return self.save(id, obj)
}

func (self *DB) update(id string, old, obj interface{}) error {
	return self.save(id, obj)
}

func (self *DB) Set(obj interface{}) error {
	ptrValue := reflect.ValueOf(obj)
	if ptrValue.Kind() != reflect.Ptr {
		return fmt.Errorf("%v is not a pointer", obj)
	}
	value := ptrValue.Elem()
	if value.Kind() != reflect.Struct {
		return fmt.Errorf("%v is not a pointer to a struct", obj)
	}
	id := value.FieldByName("Id")
	if id.Kind() == reflect.Invalid {
		return fmt.Errorf("%v does not have an Id field", obj)
	}
	if !id.CanSet() {
		return fmt.Errorf("%v can not assign its Id field", obj)
	}
	if id.Kind() != reflect.String {
		return fmt.Errorf("%v does not have a string Id field", obj)
	}
	old := reflect.New(value.Type()).Interface()
	if err := self.Get(id.String(), old); err == nil {
		return self.update(id.String(), old, obj)
	} else {
		if err != NotFound {
			return err
		}
		return self.create(id, obj)
	}
}
