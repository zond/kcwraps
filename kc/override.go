package kc

import (
	"bitbucket.org/ww/cabinet"
)

type Cursor struct {
	cabinet.KCCUR
	db *DB
}

func (self *Cursor) Db() (kc *DB) {
	return self.db
}
func (self *Cursor) Get(advance bool) (k, v []byte, err error) {
	k, v, err = self.KCCUR.Get(advance)
	k = unescape(k)
	return
}
func (self *Cursor) GetKey(advance bool) (k []byte, err error) {
	k, err = self.KCCUR.GetKey(advance)
	k = unescape(k)
	return
}
func (self *Cursor) JumpBackKey(key []byte) (err error) {
	return self.KCCUR.JumpBackKey(escape(key))
}
func (self *Cursor) JumpKey(key []byte) (err error) {
	return self.KCCUR.JumpKey(escape(key))
}

func (self *DB) Add(key, value []byte) (err error) {
	return self.KCDB.Add(escape(key), value)
}
func (self *DB) Append(key, value []byte) (err error) {
	return self.KCDB.Append(escape(key), value)
}
func (self *DB) Cas(key, oval, nval []byte) (err error) {
	return self.KCDB.Cas(escape(key), oval, nval)
}
func (self *DB) Cursor() (kcc *Cursor) {
	cur := self.KCDB.Cursor()
	return &Cursor{
		KCCUR: *cur,
		db:    self,
	}
}
func (self *DB) Get(key []byte) (value []byte, err error) {
	return self.KCDB.Get(escape(key))
}
func (self *DB) IncrDouble(key []byte, amount float64) (err error) {
	return self.KCDB.IncrDouble(escape(key), amount)
}
func (self *DB) IncrInt(key []byte, amount int64) (result int64, err error) {
	return self.KCDB.IncrInt(escape(key), amount)
}
func (self *DB) Keys() (out chan []byte) {
	out = make(chan []byte)
	go func() {
		for key := range self.KCDB.Keys() {
			out <- unescape(key)
		}
	}()
	return
}
func (self *DB) MatchPrefix(prefix string, max int) (matches [][]byte, err error) {
	matches, err = self.KCDB.MatchPrefix(prefix, max)
	for index, match := range matches {
		matches[index] = unescape(match)
	}
	return
}
func (self *DB) MatchRegex(regex string, max int) (matches [][]byte, err error) {
	matches, err = self.KCDB.MatchRegex(regex, max)
	for index, match := range matches {
		matches[index] = unescape(match)
	}
	return
}
func (self *DB) Merge(dbs []*DB, mode int) (err error) {
	sdbs := make([]*cabinet.KCDB, len(dbs))
	for index, db := range dbs {
		sdbs[index] = &db.KCDB
	}
	return self.KCDB.Merge(sdbs, mode)
}
func (self *DB) Remove(key []byte) (err error) {
	return self.KCDB.Remove(escape(key))
}
func (self *DB) Replace(key, value []byte) (err error) {
	return self.KCDB.Replace(escape(key), value)
}
func (self *DB) Set(key, value []byte) (err error) {
	return self.KCDB.Set(escape(key), value)
}
