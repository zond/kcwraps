package kc

import (
	"bitbucket.org/ww/cabinet"
	"fmt"
)

func escape(bs []byte) (result []byte) {
	for index := 0; index < len(bs); index++ {
		if bs[index] == 0 {
			result = append(result, 0, 0)
		} else {
			result = append(result, bs[index])
		}
	}
	result = append(result, 0, 1)
	return
}

func JoinKeys(keys [][]byte) (result []byte) {
	for _, key := range keys {
		result = append(result, escape(key)...)
	}
	return
}

func SplitKeys(key []byte) (result [][]byte) {
	var last []byte
	for index := 0; index < len(key); index++ {
		if key[index] == 0 {
			if key[index+1] == 1 {
				result = append(result, last)
				last = nil
			} else {
				last = append(last, 0)
			}
			index++
		} else {
			last = append(last, key[index])
		}
	}
	return
}

func Keyify(keys ...interface{}) (result [][]byte) {
	for _, key := range keys {
		if str, ok := key.(string); ok {
			result = append(result, []byte(str))
		} else if b, ok := key.([]byte); ok {
			result = append(result, b)
		} else {
			panic(fmt.Errorf("Can only Keyify strings and bytes slices, not %v", keys))
		}
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
