package kc

import (
	"bytes"
)

/*
KV is a key/value pair
*/
type KV struct {
	Keys  [][]byte
	Value []byte
}

/*
SubClear removes all values under keys.
*/
func (self *DB) ClearAll(keys [][]byte) {
	self.each(keys, func(keys1 [][]byte, v []byte) {
		self.Remove(keys1)
	})
}

/*
GetCollections returns the sorted key/value pairs under keys.
*/
func (self *DB) GetCollection(keys [][]byte) (result []KV) {
	self.each(keys, func(keys1 [][]byte, v []byte) {
		result = append(result, KV{
			Keys:  keys1,
			Value: v,
		})
	})
	return
}

func (self *DB) each(keys [][]byte, f func(keys [][]byte, value []byte)) {
	joined := join(keys)
	cursor := self.KCDB.Cursor()
	var err error
	if err = cursor.JumpKey(joined); err != nil {
		if err.Error() == "no record" {
			return
		}
		panic(err)
	}
	for {
		key, value, err := cursor.Get(true)
		if err != nil {
			if err.Error() == "no record" {
				return
			}
			panic(err)
		}
		if len(key) > len(joined) && bytes.Compare(joined, key[:len(joined)]) == 0 {
			f(split(key), value)
		} else {
			break
		}
	}
	if err != nil {
		if err.Error() == "no record" {
			return
		}
		panic(err)
	}
}
