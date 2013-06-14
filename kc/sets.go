package kc

import (
	"bytes"
	"fmt"
)

func keyJoin(key1, key2 []byte) (result []byte) {
	escapedKey1 := escape(key1)
	result = make([]byte, len(escapedKey1)+len(key2)+1)
	copy(result, escapedKey1)
	copy(result[len(escapedKey1)+1:], key2)
	return
}

func keySplit(key []byte) (key1, key2 []byte, err error) {
	for index := 0; index < len(key); index++ {
		if key[index] == 0 {
			if index == len(key)-1 || key[index+1] != 0 {
				return unescape(key[:index]), key[index+1:], nil
			} else {
				index++
			}
		}
	}
	return key, nil, fmt.Errorf("%v is not a key pair, found no single 0", key)
}

/*
KV is a key/value pair
*/
type KV struct {
	Key   []byte
	Value []byte
}

/*
SubSet sets key1/key2 to value.
*/
func (self *DB) SubSet(key1, key2, value []byte) error {
	return self.KCDB.Set(keyJoin(key1, key2), value)
}

/*
SubGet returns the value under key1/key2.
*/
func (self *DB) SubGet(key1, key2 []byte) ([]byte, error) {
	return self.KCDB.Get(keyJoin(key1, key2))
}

/*
SubRemove removes the value under key1/key2.
*/
func (self *DB) SubRemove(key1, key2 []byte) error {
	return self.KCDB.Remove(keyJoin(key1, key2))
}

/*
SubCas compares and swaps the value under key1/key2.
*/
func (self *DB) SubCas(key1, key2, old, neu []byte) error {
	return self.KCDB.Cas(keyJoin(key1, key2), old, neu)
}

/*
SubIncrDouble increments the float64 under key1/key2.
*/
func (self *DB) SubIncrDouble(key1, key2 []byte, delta float64) error {
	return self.KCDB.IncrDouble(keyJoin(key1, key2), delta)
}

/*
DubIncrFloat increments the int64 under key1/key2.
*/
func (self *DB) SubIncrInt(key1, key2 []byte, delta int64) (int64, error) {
	return self.KCDB.IncrInt(keyJoin(key1, key2), delta)
}

/*
SubClear removes all values under key1.
*/
func (self *DB) SubClear(key1 []byte) {
	self.each(key1, func(k1, k2, v []byte) {
		self.SubRemove(k1, k2)
	})
}

/*
GetCollections returns the sorted key/value pairs under key1.
*/
func (self *DB) GetCollection(key1 []byte) (result []KV) {
	self.each(key1, func(k1, k2, v []byte) {
		result = append(result, KV{
			Key:   k2,
			Value: v,
		})
	})
	return
}

func (self *DB) each(key1 []byte, f func(key1, key2, value []byte)) {
	cursor := self.KCDB.Cursor()
	var err error
	if err = cursor.JumpKey(key1); err != nil {
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
		if foundKey1, foundKey2, err := keySplit(key); err == nil {
			if bytes.Compare(key1, foundKey1) == 0 {
				f(key1, foundKey2, value)
			} else {
				break
			}
		} else if bytes.Compare(unescape(key), key1) > 0 {
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
