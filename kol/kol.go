package kol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/zond/kcwraps/kc"
)

const (
	primaryKey     = "pk"
	kol            = "kol"
	idField        = "Id"
	updatedAtField = "UpdatedAt"
	createdAtField = "CreatedAt"
)

var timeType = reflect.TypeOf(time.Now())

type Id []byte

func (self Id) String() string {
	return base64.URLEncoding.EncodeToString(self)
}

func DecodeId(s string) (result Id, err error) {
	b, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return
	}
	result = Id(b)
	return
}

func (self Id) MarshalJSON() (b []byte, err error) {
	return json.Marshal(self.String())
}

func (self *Id) UnmarshalJSON(b []byte) (err error) {
	base64Encoded := ""
	if err = json.Unmarshal(b, &base64Encoded); err != nil {
		return
	}
	*self, err = base64.URLEncoding.DecodeString(base64Encoded)
	return
}

func (self *Id) Equals(o Id) bool {
	return bytes.Compare([]byte(*self), []byte(o)) == 0
}

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
	db                 *kc.DB
	subscriptionsMutex *sync.RWMutex
	subscriptions      map[string]map[string]*Subscription
}

// Must returns a new object layer with a databsae at the specified path, or panics.
func Must(path string) *DB {
	if result, err := New(path); err == nil {
		return result
	} else {
		panic(err)
	}
}

// New returns a new object layer with a database at the specified path.
func New(path string) (result *DB, err error) {
	var kcdb *kc.DB
	if kcdb, err = kc.New(path); err != nil {
		return
	}
	result = &DB{
		db:                 kcdb,
		subscriptionsMutex: new(sync.RWMutex),
		subscriptions:      make(map[string]map[string]*Subscription),
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

/*
BetweenTransactions will run f at once if the DB is not inside a transaction,
or run it after the current transaction is finished if it is inside a transaction.
*/
func (self DB) BetweenTransactions(f func(db *DB)) {
	self.db.BetweenTransactions(func(d *kc.DB) {
		self.db = d
		f(&self)
	})
}

/*
Transact will execute f, with d being a *DB executing within a transactional context.

If self is already in a transactional context, no new transaction will be created,
f will just execute within the same transaction.
*/
func (self DB) Transact(f func(d *DB) error) (err error) {
	return self.db.Transact(func(d *kc.DB) error {
		self.db = d
		return f(&self)
	})
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
	if err = self.Transact(func(self *DB) error {
		b, err := self.db.Get(kc.Keyify(primaryKey, typ.Name(), id.Bytes()))
		if err == nil {
			if err := json.Unmarshal(b, obj); err != nil {
				return err
			}
			if err := self.deIndex(id.Bytes(), value, typ); err != nil {
				return err
			}
		} else if err.Error() != kc.NoRecord {
			return err
		}
		if err := self.db.Remove(kc.Keyify(primaryKey, typ.Name(), id.Bytes())); err != nil {
			if err.Error() == kc.NoRecord {
				err = NotFound
			}
			return err
		}
		return nil
	}); err == nil {
		self.db.BetweenTransactions(func(d *kc.DB) {
			self.emit(typ, &value, nil)
		})
	}
	return
}

/*
Get will find the object from the database, and JSON decode it into result.

Obj must be a pointer to a struct having a []byte Id field.
*/
func (self *DB) Get(obj interface{}) error {
	value, id, err := identify(obj)
	if err != nil {
		return err
	}
	return self.get(id.Bytes(), value, obj)
}

func (self *DB) get(id []byte, value reflect.Value, obj interface{}) error {
	b, err := self.db.Get(kc.Keyify(primaryKey, value.Type().Name(), id))
	if err != nil {
		if err.Error() == kc.NoRecord {
			err = NotFound
		}
		return err
	}
	return json.Unmarshal(b, obj)
}

func (self *DB) save(id []byte, typ reflect.Type, obj interface{}) error {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return self.db.Set(kc.Keyify(primaryKey, typ.Name(), id), bytes)
}

func (self *DB) create(id []byte, value reflect.Value, typ reflect.Type, obj interface{}) error {
	if updatedAt := value.FieldByName(updatedAtField); updatedAt.IsValid() && updatedAt.Type() == timeType {
		updatedAt.Set(reflect.ValueOf(time.Now()))
	}
	if createdAt := value.FieldByName(createdAtField); createdAt.IsValid() && createdAt.Type() == timeType {
		createdAt.Set(reflect.ValueOf(time.Now()))
	}
	if err := self.Transact(func(self *DB) error {
		if err := self.index(id, value, typ); err != nil {
			return err
		}
		return self.save(id, typ, obj)
	}); err != nil {
		return err
	}
	self.db.BetweenTransactions(func(d *kc.DB) {
		self.emit(typ, nil, &value)
	})
	return nil
}

func (self *DB) update(id []byte, oldValue, objValue reflect.Value, typ reflect.Type, obj interface{}) error {
	if updatedAt := objValue.FieldByName(updatedAtField); updatedAt.IsValid() && updatedAt.Type() == timeType {
		updatedAt.Set(reflect.ValueOf(time.Now()))
	}
	if err := self.Transact(func(self *DB) error {
		if err := self.deIndex(id, oldValue, typ); err != nil {
			return err
		}
		if err := self.index(id, objValue, typ); err != nil {
			return err
		}
		return self.save(id, typ, obj)
	}); err != nil {
		return err
	}
	self.db.BetweenTransactions(func(d *kc.DB) {
		self.emit(typ, &oldValue, &objValue)
	})
	return nil
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
		return self.create(idBytes, value, value.Type(), obj)
	} else {
		typ := value.Type()
		old := reflect.New(typ).Interface()
		oldValue := reflect.ValueOf(old).Elem()
		var oldValuePtr *reflect.Value
		return self.Transact(func(self *DB) error {
			if err := self.get(idBytes, oldValue, old); err == nil {
				oldValuePtr = &oldValue
				return self.update(idBytes, oldValue, value, typ, obj)
			} else {
				if err != NotFound {
					return err
				}
				return self.create(idBytes, value, value.Type(), obj)
			}
		})
	}
}
