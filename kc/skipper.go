package kc

import (
	"bitbucket.org/ww/cabinet"
	"bytes"
	"github.com/zond/setop"
	"math/big"
)

type kcSkipper struct {
	cursor *cabinet.KCCUR
	key    []byte
	length int
}

func minimum(result int, slice ...int) int {
	for _, i := range slice {
		if i < result {
			result = i
		}
	}
	return result
}

func (self *kcSkipper) skip(min []byte, gt int, maxLengths ...int) (key, value []byte, found bool, err error) {
	if key, value, err = self.cursor.Get(false); err != nil {
		if err.Error() == NoRecord {
			err = nil
		}
		return
	}
	if bytes.Compare(key[:minimum(len(key), maxLengths...)], min) > gt {
		found = true
		return
	}
	if err = self.cursor.JumpKey(min); err != nil {
		if err.Error() == NoRecord {
			err = nil
		}
		return
	}
	if key, value, err = self.cursor.Get(false); err != nil {
		if err.Error() == NoRecord {
			err = nil
		}
		return
	}
	if bytes.Compare(key[:minimum(len(key), maxLengths...)], min) > gt {
		found = true
		return
	}
	if len(maxLengths) == 0 {
		if err = self.cursor.Step(); err != nil {
			if err.Error() == NoRecord {
				err = nil
			}
			return
		}
	} else {
		min = big.NewInt(0).Add(big.NewInt(0).SetBytes(min), big.NewInt(1)).Bytes()
		if err = self.cursor.JumpKey(min); err != nil {
			if err.Error() == NoRecord {
				err = nil
			}
			return
		}
	}
	if key, value, err = self.cursor.Get(false); err != nil {
		if err.Error() == NoRecord {
			err = nil
		}
		return
	}
	if bytes.Compare(key[:minimum(len(key), maxLengths...)], min) > gt {
		found = true
	}
	return
}

func (self *kcSkipper) Skip(min []byte, inc bool) (result *setop.SetOpResult, err error) {
	gt := 0
	if inc {
		gt = -1
	}

	var maxLengths []int
	var realMin []byte
	if min == nil {
		// The real key for this min value is whatever after our key
		realMin = self.key
		gt = 0
	} else {
		// Calculate the real key for this min value by appending it to our superkey
		escapedMin := escape(min)
		realMin = make([]byte, len(self.key)+len(escapedMin))
		copy(realMin, self.key)
		copy(realMin[len(self.key):], escapedMin)
		maxLengths = []int{len(realMin)}
	}

	var key []byte
	var value []byte
	var found bool

	if key, value, found, err = self.skip(realMin, gt, maxLengths...); err != nil || !found {
		return
	}

	// If we reached a key not prefixed by our superkey, we have nothing more to give
	if len(key) <= len(self.key) || bytes.Compare(self.key, key[:len(self.key)]) != 0 {
		// Not part of our set, wrong super keys
		return
	}

	// Where are we at, then
	splitKey := SplitKeys(key)

	// Good data, return it
	result = &setop.SetOpResult{
		Key:    splitKey[self.length],
		Values: [][]byte{value},
	}
	return
}
