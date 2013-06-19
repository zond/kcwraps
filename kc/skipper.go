package kc

import (
	"bitbucket.org/ww/cabinet"
	"bytes"
	"github.com/zond/setop"
)

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
					if err.Error() == "no record" {
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
		splitKey := SplitKeys(key)

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
			if err.Error() == "no record" {
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
