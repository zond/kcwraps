package kc

import (
	"fmt"
	"github.com/fsouza/gokabinet/kc"
)

func keyCombine(key1, key2 string) string {
	return fmt.Sprintf("%v\000%v", key1, key2)
}

type DB struct {
	kc.DB
}

func New(path string) (result *DB, err error) {
	var kdb *kc.DB
	if kdb, err = kc.Open(fmt.Sprintf("%v.kct", path), kc.WRITE); err != nil {
		return
	}
	result = &DB{
		DB: *kdb,
	}
	return
}

func (self *DB) SubSetInt(key1, key2 string, value int) error {
	return self.DB.SetInt(keyCombine(key1, key2), value)
}

func (self *DB) SubGetInt(key1, key2 string) (int, error) {
	return self.DB.GetInt(keyCombine(key1, key2))
}

func (self *DB) SubSetGob(key1, key2 string, value interface{}) error {
	return self.DB.SetGob(keyCombine(key1, key2), value)
}

func (self *DB) SubGetGob(key1, key2 string, result interface{}) error {
	return self.DB.GetGob(keyCombine(key1, key2), result)
}

func (self *DB) SubSet(key1, key2, value string) error {
	return self.DB.Set(keyCombine(key1, key2), value)
}

func (self *DB) SubGet(key1, key2 string) (string, error) {
	return self.DB.Get(keyCombine(key1, key2))
}

func (self *DB) SubRemove(key1, key2 string) error {
	return self.DB.Remove(keyCombine(key1, key2))
}
