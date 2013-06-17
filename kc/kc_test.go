package kc

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randBytes() (result []byte) {
	result = make([]byte, rand.Int()%256)
	for index, _ := range result {
		result[index] = byte(rand.Int() % 256)
	}
	return
}

func TestSplitJoin1(t *testing.T) {
	keys := [][]byte{[]byte{0, 1}, []byte{1, 2}}
	joined := join(keys)
	wanted := []byte{0, 0, 1, 0, 1, 1, 2, 0, 1}
	if bytes.Compare(joined, wanted) != 0 {
		t.Fatalf("%v != %v", join(keys), wanted)
	}
	splitted := split(joined)
	if len(splitted) != len(keys) {
		t.Fatalf("%v != %v", splitted, keys)
	}
	for index, _ := range keys {
		if bytes.Compare(keys[index], splitted[index]) != 0 {
			t.Fatalf("%v != %v", keys[index], splitted[index])
		}
	}
}

func TestSplitJoin2(t *testing.T) {
	for i := 0; i < 1000; i++ {
		for j := 0; j < 10; j++ {
			var keys [][]byte
			for k := 0; k < j; k++ {
				keys = append(keys, randBytes())
			}
			keys2 := split(join(keys))
			if len(keys) != len(keys2) {
				t.Fatalf("%v != %v", keys, keys2)
			}
			for index, _ := range keys2 {
				if bytes.Compare(keys[index], keys2[index]) != 0 {
					t.Fatalf("%v != %v", keys[index], keys2[index])
				}
			}
		}
	}
}

func TestCrud(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer d.Close()
	d.Clear()
	if err := d.Set(Keyify("k1", "k2"), []byte("v")); err != nil {
		t.Errorf(err.Error())
	}
	if v, err := d.Get(Keyify("k1", "k2")); string(v) != "v" || err != nil {
		t.Errorf("Wrong value!")
	}
	if err := d.Remove(Keyify("k1", "k2")); err != nil {
		t.Errorf(err.Error())
	}
	if v, err := d.Get(Keyify("k1", "k2")); string(v) == "v" || err == nil {
		t.Errorf("Not removed!")
	}
}

func TestError(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer d.Close()
	d.Clear()
	if err := d.GetCollection(Keyify("hehu")); err != nil {
		t.Errorf("%#v", err)
	}
}

func TestCollection1(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer d.Close()
	d.Clear()
	d.Set(Keyify("a", "b"), []byte("1"))
	d.Set(Keyify("x", "b"), []byte("c"))
	d.Set(Keyify("x", "c"), []byte("d"))
	d.Set(Keyify("x", "d"), []byte("e"))
	d.Set(Keyify("z", "b"), []byte("1"))
	coll := d.GetCollection(Keyify("x"))
	wanted := []KV{
		KV{
			Keys:  Keyify("x", "b"),
			Value: []byte("c"),
		},
		KV{
			Keys:  Keyify("x", "c"),
			Value: []byte("d"),
		},
		KV{
			Keys:  Keyify("x", "d"),
			Value: []byte("e"),
		},
	}
	if !reflect.DeepEqual(coll, wanted) {
		t.Fatalf("%v != %v", coll, wanted)
	}
	d.ClearAll(Keyify("x"))
	coll = d.GetCollection(Keyify("x"))
	if len(coll) != 0 {
		t.Errorf("Wanted 0 elements")
	}
}

func TestCollection2(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer d.Close()
	d.Clear()
	d.Set(Keyify("a"), []byte("1"))
	d.Set(Keyify("x", "b"), []byte("c"))
	d.Set(Keyify("x", "c"), []byte("d"))
	d.Set(Keyify("x", "d"), []byte("e"))
	d.Set(Keyify("z"), []byte("1"))
	coll := d.GetCollection(Keyify("x"))
	wanted := []KV{
		KV{
			Keys:  Keyify("x", "b"),
			Value: []byte("c"),
		},
		KV{
			Keys:  Keyify("x", "c"),
			Value: []byte("d"),
		},
		KV{
			Keys:  Keyify("x", "d"),
			Value: []byte("e"),
		},
	}
	if !reflect.DeepEqual(coll, wanted) {
		t.Fatalf("%v != %v", coll, wanted)
	}
	d.ClearAll(Keyify("x"))
	coll = d.GetCollection(Keyify("x"))
	if len(coll) != 0 {
		t.Errorf("Wanted 0 elements")
	}
}

func TestCollection3(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer d.Close()
	d.Clear()
	d.Set([][]byte{[]byte{0, 1}, []byte{0, 0}}, []byte("c"))
	d.Set([][]byte{[]byte{0, 1}, []byte{0, 1}}, []byte("d"))
	d.Set([][]byte{[]byte{0, 1}, []byte{1, 0}}, []byte("e"))
	coll := d.GetCollection([][]byte{[]byte{0, 1}})
	wanted := []KV{
		KV{
			Keys:  [][]byte{[]byte{0, 1}, []byte{0, 0}},
			Value: []byte("c"),
		},
		KV{
			Keys:  [][]byte{[]byte{0, 1}, []byte{0, 1}},
			Value: []byte("d"),
		},
		KV{
			Keys:  [][]byte{[]byte{0, 1}, []byte{1, 0}},
			Value: []byte("e"),
		},
	}
	if !reflect.DeepEqual(coll, wanted) {
		t.Fatalf("%v != %v", coll, wanted)
	}
	d.ClearAll([][]byte{[]byte{0, 1}})
	coll = d.GetCollection(Keyify("x"))
	if len(coll) != 0 {
		t.Errorf("Wanted 0 elements")
	}
}

func TestMultiLevelCollection(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer d.Close()
	d.Clear()
	d.Set(Keyify("a", "b", "c"), []byte("d"))
	d.Set(Keyify("a", "b", "d"), []byte("e"))
	d.Set(Keyify("a", "b", "e"), []byte("f"))
	d.Set(Keyify("a", "c", "f"), []byte("g"))
	coll := d.GetCollection(Keyify("a"))
	if len(coll) != 0 {
		t.Fatalf("wanted empty result, got %v", coll)
	}
	coll = d.GetCollection(Keyify("a", "c"))
	wanted := []KV{
		KV{
			Keys:  Keyify("a", "c", "f"),
			Value: []byte("g"),
		},
	}
	if !reflect.DeepEqual(coll, wanted) {
		t.Fatalf("%#v != %v", coll, wanted)
	}
	coll = d.GetCollection(Keyify("a", "b"))
	wanted = []KV{
		KV{
			Keys:  Keyify("a", "b", "c"),
			Value: []byte("d"),
		},
		KV{
			Keys:  Keyify("a", "b", "d"),
			Value: []byte("e"),
		},
		KV{
			Keys:  Keyify("a", "b", "e"),
			Value: []byte("f"),
		},
	}
	if !reflect.DeepEqual(coll, wanted) {
		t.Fatalf("%#v != %v", coll, wanted)
	}
	d.ClearAll(Keyify("a"))
	coll = d.GetCollection(Keyify("a"))
	if len(coll) != 0 {
		t.Errorf("Wanted 0 elements")
	}
}

func TestSetOps1(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer d.Close()
	d.Clear()
	d.Set(Keyify("a", "b"), []byte("c"))
	d.Set(Keyify("a", "c"), []byte("d"))
	d.Set(Keyify("b", "c"), []byte("e"))
	d.Set(Keyify("b", "d"), []byte("f"))
	if !reflect.DeepEqual(d.SetOp("(I:ConCat a b)"), []KV{
		KV{
			Keys:  Keyify("c"),
			Value: []byte("de"),
		},
	}) {
		t.Errorf("Bad result")
	}
}

func TestSetOps2(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer d.Close()
	d.Clear()
	d.Set(Keyify("a", "b", "a"), []byte("c"))
	d.Set(Keyify("a", "b", "b"), []byte("c"))
	d.Set(Keyify("a", "b", "c"), []byte("c"))
	d.Set(Keyify("b", "c", "c"), []byte("e"))
	d.Set(Keyify("b", "d", "c"), []byte("f"))
	d.Set(Keyify("b", "d", "d"), []byte("f"))
	found := d.SetOp("(I:ConCat a/b b/c b/d)")
	wanted := []KV{
		KV{
			Keys:  Keyify("c"),
			Value: []byte("cef"),
		},
	}
	if !reflect.DeepEqual(found, wanted) {
		t.Errorf("%+v != %+v", found, wanted)
	}
}
