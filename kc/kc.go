package kc

import (
	"bitbucket.org/ww/cabinet"
	"fmt"
)

func zeroes(bs []byte) (result int) {
	for _, b := range bs {
		if b == 0 {
			result++
		}
	}
	return
}

func escape(bs []byte) (result []byte) {
	result = make([]byte, len(bs)+zeroes(bs))
	bsIndex := 0
	resultIndex := 0
	for bsIndex < len(bs) {
		if bs[bsIndex] == 0 {
			result[resultIndex], result[resultIndex+1] = 0, 0
			resultIndex++
		} else {
			result[resultIndex] = bs[bsIndex]
		}
		resultIndex++
		bsIndex++
	}
	return
}

func unescape(bs []byte) (result []byte) {
	result = make([]byte, len(bs)-(zeroes(bs)/2))
	bsIndex := 0
	resultIndex := 0
	for bsIndex < len(bs) {
		if bs[bsIndex] == 0 && bs[bsIndex+1] == 0 {
			result[resultIndex] = 0
			bsIndex++
		} else {
			result[resultIndex] = bs[bsIndex]
		}
		resultIndex++
		bsIndex++
	}
	return
}

/*
DB includes http://godoc.org/bitbucket.org/ww/cabinet#KCDB and adds a few more convenience functions.
*/
type DB struct {
	cabinet.KCDB
}

/*
New returns a new DB. Since the whole point of this package requires the DB to have a tree database, the
path gets '.kct' appended to ensure that it will be a tree database. Thus: don't provide a suffix to your path.
*/
func New(path string) (result *DB, err error) {
	kcdb := cabinet.New()
	if err = kcdb.Open(fmt.Sprintf("%v.kct", path), cabinet.KCOWRITER|cabinet.KCOCREATE); err != nil {
		return
	}
	result = &DB{
		KCDB: *kcdb,
	}
	return
}
