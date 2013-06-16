package kc

import (
	"bitbucket.org/ww/cabinet"
	"bytes"
	"github.com/zond/setop"
	"strings"
)

/*
KV is a key/value pair
*/
type KV struct {
	Keys  [][]byte
	Value []byte
}

type kcSkipper struct {
	cursor *cabinet.KCCUR
	key    []byte
	length int
}

func (self *kcSkipper) Skip(min []byte, inc bool) (result *setop.SetOpResult, err error) {
	lt := 1
	if inc {
		lt = 0
	}

	// Calculate the real key for this min value by appending it to our superkey
	escapedMin := escape(min)
	realMin := make([]byte, len(self.key)+len(escapedMin))
	copy(realMin, self.key)
	copy(realMin[len(self.key):], escapedMin)

	var key []byte
	var value []byte

	if key, value, err = self.cursor.Get(false); err != nil { // Check where we are at now
		if err.Error() == "no record" {
			err = nil
		}
		// Error or no more data
		return
	}

	if bytes.Compare(key, self.key) < 1 { // If we are not yet in our set, skip to the starting position
		if err = self.cursor.JumpKey(realMin); err != nil {
			if err.Error() == "no record" {
				err = nil
			}
			// Error or no more data
			return
		}

		// And refetch where we are
		if key, value, err = self.cursor.Get(false); err != nil {
			if err.Error() == "no record" {
				err = nil
			}
			// Error or no more data
			return
		}
	} else {
		cmp := bytes.Compare(key, realMin)
		if min != nil && cmp < lt { // If we past our starting position, but are not far enough ahead, skip a bit
			if cmp < 0 { // If we are below realMin, jump
				if err = self.cursor.JumpKey(realMin); err != nil {
					if err.Error() == "no record" {
						err = nil
					}
					// Error or no more data
					return
				}
			} else { // otherwise, we are AT realMin but we want to be over it, step
				if err = self.cursor.Step(); err != nil {
					if err.Error() == "no error" {
						err = nil
					}
					// Error or no more data
					return
				}
			}
		}

		// And refetch where we are
		if key, value, err = self.cursor.Get(false); err != nil {
			if err.Error() == "no record" {
				err = nil
			}
			// Error or no more data
			return
		}

	}

	for {
		// If we reached a key not prefixed by our superkey, we have nothing more to give
		if len(key) < len(self.key) || bytes.Compare(self.key, key[:len(self.key)]) != 0 {
			// Not part of our set, wrong super keys
			return
		}

		// Where are we at, then
		splitKey := split(key)

		// If we are in OUR set
		if len(splitKey) == self.length+1 {
			// Good data, return it
			result = &setop.SetOpResult{
				Key:    splitKey[len(splitKey)-1],
				Values: [][]byte{value},
			}
			return
		}

		// Otherwise we are in a subset of our set, just step along
		if err = self.cursor.Step(); err != nil {
			if err.Error() == "no error" {
				err = nil
			}
			// Error or no more data
			return
		}
		if key, value, err = self.cursor.Get(false); err != nil {
			if err.Error() == "no record" {
				err = nil
			}
			// Error or no more data
			return
		}
	}
}

func (self *DB) skipper(b []byte) setop.Skipper {
	keyParts := strings.Split(string(b), "/")
	keys := make([][]byte, len(keyParts))
	for index, key := range keyParts {
		keys[index] = []byte(key)
	}
	key := join(keys)
	result := &kcSkipper{
		cursor: self.KCDB.Cursor(),
		length: len(keys),
		key:    key,
	}
	return result
}

func (self *DB) SetOp(expr string) (result []KV) {
	if err := (&setop.SetExpression{
		Code: expr,
	}).Each(self.skipper, func(res *setop.SetOpResult) {
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
			splitKey := split(key)
			if len(splitKey) == len(keys)+1 {
				f(split(key), value)
			}
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
