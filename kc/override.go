package kc

import (
	"bitbucket.org/ww/cabinet"
)

// Cursor is just an extension of http://godoc.org/bitbucket.org/ww/cabinet#KCCUR with support for the multi level keys.
// All functions that process keys have been overridden to use the multi level key scheme.
type Cursor struct {
	cabinet.KCCUR
	db *DB
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCCUR.Db
func (self *Cursor) Db() (kc *DB) {
	return self.db
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCCUR.Get
func (self *Cursor) Get(advance bool) (k [][]byte, v []byte, err error) {
	var k0 []byte
	if k0, v, err = self.KCCUR.Get(advance); err == nil {
		k = SplitKeys(k0)
	}
	return
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCCUR.GetKey
func (self *Cursor) GetKey(advance bool) (k [][]byte, err error) {
	var k0 []byte
	if k0, err = self.KCCUR.GetKey(advance); err == nil {
		k = SplitKeys(k0)
	}
	return
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCCUR.JumpBackKey
func (self *Cursor) JumpBackKey(keys ...[]byte) (err error) {
	return self.KCCUR.JumpBackKey(JoinKeys(keys))
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCCUR.JumpKey
func (self *Cursor) JumpKey(keys ...[]byte) (err error) {
	return self.KCCUR.JumpKey(JoinKeys(keys))
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.Add
func (self *DB) Add(keys [][]byte, value []byte) (err error) {
	return self.KCDB.Add(JoinKeys(keys), value)
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.Append
func (self *DB) Append(keys [][]byte, value []byte) (err error) {
	return self.KCDB.Append(JoinKeys(keys), value)
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.Cas
func (self *DB) Cas(keys [][]byte, oval, nval []byte) (err error) {
	return self.KCDB.Cas(JoinKeys(keys), oval, nval)
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.Cursor
func (self *DB) Cursor() (kcc *Cursor) {
	cur := self.KCDB.Cursor()
	return &Cursor{
		KCCUR: *cur,
		db:    self,
	}
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.Get
func (self *DB) Get(keys [][]byte) (value []byte, err error) {
	return self.KCDB.Get(JoinKeys(keys))
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.IncrDouble
func (self *DB) IncrDouble(keys [][]byte, amount float64) (err error) {
	return self.KCDB.IncrDouble(JoinKeys(keys), amount)
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.IncrInt
func (self *DB) IncrInt(keys [][]byte, amount int64) (result int64, err error) {
	return self.KCDB.IncrInt(JoinKeys(keys), amount)
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.Keys
func (self *DB) Keys() (out chan [][]byte) {
	out = make(chan [][]byte)
	go func() {
		for key := range self.KCDB.Keys() {
			out <- SplitKeys(key)
		}
	}()
	return
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.MatchPrefix
func (self *DB) MatchPrefix(prefix string, max int) (matches [][][]byte, err error) {
	var matches0 [][]byte
	if matches0, err = self.KCDB.MatchPrefix(string(escape([]byte(prefix))), max); err == nil {
		for _, match := range matches0 {
			matches = append(matches, SplitKeys(match))
		}
	}
	return
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.MatchRegex
func (self *DB) MatchRegex(regex string, max int) (matches [][][]byte, err error) {
	var matches0 [][]byte
	if matches0, err = self.KCDB.MatchRegex(string(escape([]byte(regex))), max); err == nil {
		for _, match := range matches0 {
			matches = append(matches, SplitKeys(match))
		}
	}
	return
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.Merge
func (self *DB) Merge(dbs []*DB, mode int) (err error) {
	sdbs := make([]*cabinet.KCDB, len(dbs))
	for index, db := range dbs {
		sdbs[index] = &db.KCDB
	}
	return self.KCDB.Merge(sdbs, mode)
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.Remove
func (self *DB) Remove(keys [][]byte) (err error) {
	return self.KCDB.Remove(JoinKeys(keys))
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.Replace
func (self *DB) Replace(keys [][]byte, value []byte) (err error) {
	return self.KCDB.Replace(JoinKeys(keys), value)
}

// http://godoc.org/bitbucket.org/ww/cabinet#KCDB.Set
func (self *DB) Set(keys [][]byte, value []byte) (err error) {
	return self.KCDB.Set(JoinKeys(keys), value)
}
