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
func (self *Cursor) Get(advance bool) (k [][]byte, v []byte, err error) {
	var k0 []byte
	if k0, v, err = self.KCCUR.Get(advance); err == nil {
		k = SplitKeys(k0)
	}
	return
}
func (self *Cursor) GetKey(advance bool) (k [][]byte, err error) {
	var k0 []byte
	if k0, err = self.KCCUR.GetKey(advance); err == nil {
		k = SplitKeys(k0)
	}
	return
}
func (self *Cursor) JumpBackKey(keys ...[]byte) (err error) {
	return self.KCCUR.JumpBackKey(JoinKeys(keys))
}
func (self *Cursor) JumpKey(keys ...[]byte) (err error) {
	return self.KCCUR.JumpKey(JoinKeys(keys))
}

func (self *DB) Add(keys [][]byte, value []byte) (err error) {
	return self.KCDB.Add(JoinKeys(keys), value)
}
func (self *DB) Append(keys [][]byte, value []byte) (err error) {
	return self.KCDB.Append(JoinKeys(keys), value)
}
func (self *DB) Cas(keys [][]byte, oval, nval []byte) (err error) {
	return self.KCDB.Cas(JoinKeys(keys), oval, nval)
}
func (self *DB) Cursor() (kcc *Cursor) {
	cur := self.KCDB.Cursor()
	return &Cursor{
		KCCUR: *cur,
		db:    self,
	}
}
func (self *DB) Get(keys [][]byte) (value []byte, err error) {
	return self.KCDB.Get(JoinKeys(keys))
}
func (self *DB) IncrDouble(keys [][]byte, amount float64) (err error) {
	return self.KCDB.IncrDouble(JoinKeys(keys), amount)
}
func (self *DB) IncrInt(keys [][]byte, amount int64) (result int64, err error) {
	return self.KCDB.IncrInt(JoinKeys(keys), amount)
}
func (self *DB) Keys() (out chan [][]byte) {
	out = make(chan [][]byte)
	go func() {
		for key := range self.KCDB.Keys() {
			out <- SplitKeys(key)
		}
	}()
	return
}
func (self *DB) MatchPrefix(prefix string, max int) (matches [][][]byte, err error) {
	var matches0 [][]byte
	if matches0, err = self.KCDB.MatchPrefix(string(escape([]byte(prefix))), max); err == nil {
		for _, match := range matches0 {
			matches = append(matches, SplitKeys(match))
		}
	}
	return
}
func (self *DB) MatchRegex(regex string, max int) (matches [][][]byte, err error) {
	var matches0 [][]byte
	if matches0, err = self.KCDB.MatchRegex(string(escape([]byte(regex))), max); err == nil {
		for _, match := range matches0 {
			matches = append(matches, SplitKeys(match))
		}
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
func (self *DB) Remove(keys [][]byte) (err error) {
	return self.KCDB.Remove(JoinKeys(keys))
}
func (self *DB) Replace(keys [][]byte, value []byte) (err error) {
	return self.KCDB.Replace(JoinKeys(keys), value)
}
func (self *DB) Set(keys [][]byte, value []byte) (err error) {
	return self.KCDB.Set(JoinKeys(keys), value)
}
