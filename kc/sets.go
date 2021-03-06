package kc

import (
	"bytes"
	"strings"

	"github.com/zond/setop"
)

/*
KV is a key/value pair
*/
type KV struct {
	Keys  [][]byte
	Value []byte
}

func (self *DB) skipper(b []byte) (result setop.Skipper, err error) {
	keys := SplitKeys(b)
	result = &kcSkipper{
		cursor: self.KCDB.Cursor(),
		length: len(keys),
		key:    b,
	}
	return
}

/*
SetOp will run expr on this DB and return the result.
*/
func (self *DB) SetOp(expr *setop.SetExpression) (result []KV) {
	if err := expr.Each(self.skipper, func(res *setop.SetOpResult) {
		result = append(result, KV{
			Keys:  [][]byte{res.Key},
			Value: res.Values[0],
		})
	}); err != nil {
		panic(err)
	}
	return
}

func (self *DB) skipperString(b []byte) (result setop.Skipper, err error) {
	keyParts := strings.Split(string(b), "/")
	keys := make([][]byte, len(keyParts))
	for index, key := range keyParts {
		keys[index] = []byte(key)
	}
	key := JoinKeys(keys)
	result = &kcSkipper{
		cursor: self.KCDB.Cursor(),
		length: len(keys),
		key:    key,
	}
	return
}

/*
SetOpString will parse and execute the provided set expression and return the matches.
*/
func (self *DB) SetOpString(expr string) (result []KV) {
	if err := (&setop.SetExpression{
		Code: expr,
	}).Each(self.skipperString, func(res *setop.SetOpResult) {
		result = append(result, KV{
			Keys:  [][]byte{res.Key},
			Value: res.Values[0],
		})
	}); err != nil {
		panic(err)
	}
	return
}

/*
ClearAll removes all values under keys.
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
	joined := JoinKeys(keys)
	cursor := self.KCDB.Cursor()
	var err error
	if err = cursor.JumpKey(joined); err != nil {
		if err.Error() == NoRecord {
			return
		}
		panic(err)
	}
	for {
		key, value, err := cursor.Get(true)
		if err != nil {
			if err.Error() == NoRecord {
				return
			}
			panic(err)
		}
		if len(key) > len(joined) && bytes.Compare(joined, key[:len(joined)]) == 0 {
			splitKey := SplitKeys(key)
			if len(splitKey) > len(keys) {
				f(splitKey, value)
			}
		} else {
			break
		}
	}
	if err != nil {
		if err.Error() == NoRecord {
			return
		}
		panic(err)
	}
}
