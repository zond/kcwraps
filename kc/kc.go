package kc

import (
	"bitbucket.org/ww/cabinet"
	"fmt"
)

const (
	NoRecord = "no record"
)

func escape(bs []byte) (result []byte) {
	for index := 0; index < len(bs); index++ {
		if bs[index] == 0 {
			result = append(result, 0, 0)
		} else {
			result = append(result, bs[index])
		}
	}
	result = append(result, 0, 1)
	return
}

// JoinKeys creates a single cabinet key from the provided keys
func JoinKeys(keys [][]byte) (result []byte) {
	for _, key := range keys {
		result = append(result, escape(key)...)
	}
	return
}

// SplitKeys splits a cabinet key into a key slice
func SplitKeys(key []byte) (result [][]byte) {
	var last []byte
	for index := 0; index < len(key); index++ {
		if key[index] == 0 {
			if key[index+1] == 1 {
				result = append(result, last)
				last = nil
			} else {
				last = append(last, 0)
			}
			index++
		} else {
			last = append(last, key[index])
		}
	}
	return
}

// Keyify is a utility to convert a set of strings and []byte to a [][]byte
func Keyify(keys ...interface{}) (result [][]byte) {
	for _, key := range keys {
		if str, ok := key.(string); ok {
			result = append(result, []byte(str))
		} else if b, ok := key.([]byte); ok {
			result = append(result, b)
		} else {
			panic(fmt.Errorf("Can only Keyify strings and bytes slices, not %v", keys))
		}
	}
	return
}

/*
DB includes http://godoc.org/bitbucket.org/ww/cabinet#KCDB and adds a few more convenience functions and support for multi level keys.
All functions that process keys have been overridden to use the multi level key scheme.
*/
type DB struct {
	*cabinet.KCDB
	inTransaction bool
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
		KCDB: kcdb,
	}
	return
}

/*
Transact will execute f, with d being a *DB executing within a transactional context.

If self is already in a transactional context, no further transacting will take place,
f will just execute within the same transaction.
*/
func (self DB) Transact(f func(d *DB) error) (err error) {
	defer func() {
		if e := recover(); e != nil {
			self.EndTran(false)
			panic(e)
		}
	}()
	if self.inTransaction {
		if err = f(&self); err != nil {
			self.EndTran(false)
		}
	} else {
		if err = self.BeginTran(false); err == nil {
			self.inTransaction = true
			if err = f(&self); err == nil {
				if err = self.EndTran(true); err != nil {
					self.EndTran(false)
				}
			} else {
				self.EndTran(false)
			}
		}
	}
	return
}
