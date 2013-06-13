package kc

import (
	"bitbucket.org/ww/cabinet"
	"bytes"
	"fmt"
)

func keyCombine(key1, key2 []byte) (result []byte) {
	result = make([]byte, len(key1)+len(key2)+1)
	copy(result, key1)
	copy(result[len(key1)+1:], key2)
	return
}

/*
KV is a key/value pair
*/
type KV struct {
	Key   []byte
	Value []byte
}

/*
DB includes http://godoc.org/bitbucket.org/ww/cabinet#KCDB and adds a few more convenience functions.
*/
type DB struct {
	cabinet.KCDB
}

/*
New returns a new DB. Since the whole point of this package requires the DB to have a tree database, the
path gets '.kct' appended to ensure that it will be a tree database. Thus: don't provide a suffix to your path.
*/
func New(path string) (result *DB, err error) {
	kcdb := cabinet.New()
	if err = kcdb.Open(fmt.Sprintf("%v.kct", path), cabinet.KCOWRITER|cabinet.KCOCREATE); err != nil {
		return
	}
	result = &DB{
		KCDB: *kcdb,
	}
	return
}

/*
SubSet sets key1/key2 to value.
*/
func (self *DB) SubSet(key1, key2, value []byte) error {
	return self.KCDB.Set(keyCombine(key1, key2), value)
}

/*
SubGet returns the value under key1/key2.
*/
func (self *DB) SubGet(key1, key2 []byte) ([]byte, error) {
	return self.KCDB.Get(keyCombine(key1, key2))
}

/*
SubRemove removes the value under key1/key2.
*/
func (self *DB) SubRemove(key1, key2 []byte) error {
	return self.KCDB.Remove(keyCombine(key1, key2))
}

/*
SubIncrDouble increments the float64 under key1/key2.
*/
func (self *DB) SubIncrDouble(key1, key2 []byte, delta float64) error {
	return self.KCDB.IncrDouble(keyCombine(key1, key2), delta)
}

/*
DubIncrFloat increments the int64 under key1/key2.
*/
func (self *DB) SubIncrInt(key1, key2 []byte, delta int64) (int64, error) {
	return self.KCDB.IncrInt(keyCombine(key1, key2), delta)
}

/*
GetCollections returns the sorted key/value pairs under key1.
*/
func (self *DB) GetCollection(key1 []byte) (result []KV) {
	cursor := self.KCDB.Cursor()
	var err error
	if err = cursor.JumpKey(key1); err != nil {
		panic(err)
	}
	for {
		key, value, err := cursor.Get(true)
		if err != nil {
			panic(err)
		}
		if len(key) <= len(key1) || key[len(key1)] != 0 || bytes.Compare(key[:len(key1)], key1) != 0 {
			break
		}
		result = append(result, KV{
			Key:   key[len(key1)+1:],
			Value: value,
		})
	}
	if err != nil {
		panic(err)
	}
	return
}
