package kol

import (
	"encoding/json"
	"fmt"
	"github.com/zond/kcwraps/kc"
	"math/rand"
	"reflect"
	"sync"
	"time"
)

const (
	primaryKey = "pk"
	kol        = "kol"
	idField    = "Id"
)

// NotFound means that the mentioned key did not exist.
var NotFound = fmt.Errorf("Not found")

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randomBytes() (result []byte) {
	result = make([]byte, 24)
	for i, _ := range result {
		result[i] = byte(rand.Int31())
	}
	return
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
	id = value.FieldByName(idField)
	if id.Kind() == reflect.Invalid {
		err = fmt.Errorf("%v does not have an Id field", obj)
		return
	}
	if !id.CanSet() {
		err = fmt.Errorf("%v can not assign its Id field", obj)
		return
	}
	if id.Kind() != reflect.Slice {
		err = fmt.Errorf("%v does not have a byte slice Id field", obj)
	}
	if id.Type().Elem().Kind() != reflect.Uint8 {
		err = fmt.Errorf("%v does not have a byte slice Id field", obj)
	}
	return
}

// DB is a simple object layer on top of Kyoto Cabinet.
type DB struct {
	db                 kc.DB
	subscriptionsMutex *sync.RWMutex
	subscriptions      map[string]subscription
}

// New returns a new object layer with a database at the specified path.
func New(path string) (result *DB, err error) {
	var kcdb *kc.DB
	if kcdb, err = kc.New(path); err != nil {
		return
	}
	result = &DB{
		db:                 *kcdb,
		subscriptionsMutex: new(sync.RWMutex),
		subscriptions:      make(map[string]subscription),
	}
	return
}

// Count returns the number of elements in the underlying Kyoto cabinet.
func (self *DB) Count() (uint64, error) {
	return self.db.Count()
}

// Query will return a new Query on this database.
func (self *DB) Query() *Query {
	return &Query{
		db: self,
	}
}

// Clear will completely empty the underlying cabinet.
func (self *DB) Clear() error {
	return self.db.Clear()
}

// Close will close and sync the underlying cabinet to disk.
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

Obj must be a pointer to a struct having a []byte Id field.
*/
func (self *DB) Del(obj interface{}) (err error) {
	var value reflect.Value
	var id reflect.Value
	if value, id, err = identify(obj); err != nil {
		return
	}
	typ := value.Type()
	if err = self.trans(func() error {
		b, err := self.db.Get(kc.Keyify(primaryKey, typ.Name(), id.Bytes()))
		if err == nil {
			if err := json.Unmarshal(b, obj); err != nil {
				return err
			}
			if err := self.deIndex(id.Bytes(), value, typ); err != nil {
				return err
			}
		} else if err.Error() != "no record" {
			return err
		}
		if err := self.db.Remove(kc.Keyify(primaryKey, typ.Name(), id.Bytes())); err != nil {
			if err.Error() == "no record" {
				err = NotFound
			}
			return err
		}
		return nil
	}); err == nil {
		self.emit(typ, &value, nil)
	}
	return
}

/*
Get will find the object with id in the database, and JSON decode it into result.

Result must be a pointer to a struct having a []byte Id field.
*/
func (self *DB) Get(id []byte, result interface{}) error {
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

func (self *DB) save(id []byte, typ reflect.Type, obj interface{}) error {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return self.db.Set(kc.Keyify(primaryKey, typ.Name(), id), bytes)
}

func (self *DB) create(id []byte, value reflect.Value, typ reflect.Type, obj interface{}, inTrans bool) error {
	creator := func() error {
		if err := self.index(id, value, typ); err != nil {
			return err
		}
		return self.save(id, typ, obj)
	}
	if inTrans {
		return creator()
	} else {
		if err := self.trans(creator); err != nil {
			return err
		}
		self.emit(typ, nil, &value)
		return nil
	}
}

func (self *DB) update(id []byte, oldValue, objValue reflect.Value, typ reflect.Type, obj interface{}) error {
	if err := self.deIndex(id, oldValue, typ); err != nil {
		return err
	}
	if err := self.index(id, objValue, typ); err != nil {
		return err
	}
	return self.save(id, typ, obj)
}

/*
Set will JSON encode obj and insert it into the database

Obj must be a pointer to a struct having a []byte Id field.

If the Id field is empty, a random Id will be chosen.

Any fields tagged `kol:"index"` will be indexed separately, and possible to search for using Query.
*/
func (self *DB) Set(obj interface{}) error {
	value, id, err := identify(obj)
	if err != nil {
		return err
	}
	if idBytes := id.Bytes(); idBytes == nil {
		idBytes = randomBytes()
		id.SetBytes(idBytes)
		return self.create(idBytes, value, value.Type(), obj, false)
	} else {
		typ := value.Type()
		old := reflect.New(typ).Interface()
		var oldValuePtr *reflect.Value
		if err := self.trans(func() error {
			if err := self.Get(idBytes, old); err == nil {
				oldValue := reflect.ValueOf(old).Elem()
				oldValuePtr = &oldValue
				return self.update(idBytes, oldValue, value, typ, obj)
			} else {
				if err != NotFound {
					return err
				}
				return self.create(idBytes, value, value.Type(), obj, true)
			}
		}); err != nil {
			return err
		} else {
			self.emit(typ, oldValuePtr, &value)
			return nil
		}
	}
}
