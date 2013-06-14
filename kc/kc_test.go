package kc

import (
	"bytes"
	"math/rand"
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

func TestEscape(t *testing.T) {
	for i := 0; i < 100000; i++ {
		b := randBytes()
		if bytes.Compare(b, unescape(escape(b))) != 0 {
			t.Fatalf("unescape(escape(%v)) => %v", unescape(escape(b)), b)
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
	if err := d.SubSet([]byte("k1"), []byte("k2"), []byte("v")); err != nil {
		t.Errorf(err.Error())
	}
	if v, err := d.SubGet([]byte("k1"), []byte("k2")); string(v) != "v" || err != nil {
		t.Errorf("Wrong value!")
	}
	if err := d.SubRemove([]byte("k1"), []byte("k2")); err != nil {
		t.Errorf(err.Error())
	}
	if v, err := d.SubGet([]byte("k1"), []byte("k2")); string(v) == "v" || err == nil {
		t.Errorf("Not removed!")
	}
}

func TestError(t *testing.T) {
	d, err := New("empty")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer d.Close()
	d.Clear()
	if err := d.GetCollection([]byte("hehu")); err != nil {
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
	d.SubSet([]byte("a"), []byte("b"), []byte("1"))
	d.SubSet([]byte("x"), []byte("b"), []byte("c"))
	d.SubSet([]byte("x"), []byte("c"), []byte("d"))
	d.SubSet([]byte("x"), []byte("d"), []byte("e"))
	d.SubSet([]byte("z"), []byte("b"), []byte("1"))
	coll := d.GetCollection([]byte("x"))
	if len(coll) != 3 {
		t.Fatalf("Wanted 3 elements, got %v", coll)
	}
	if string(coll[0].Key) != "b" || string(coll[0].Value) != "c" {
		t.Errorf("Wrong value")
	}
	if string(coll[1].Key) != "c" || string(coll[1].Value) != "d" {
		t.Errorf("Wrong value")
	}
	if string(coll[2].Key) != "d" || string(coll[2].Value) != "e" {
		t.Errorf("Wrong value")
	}
	d.SubClear([]byte("x"))
	coll = d.GetCollection([]byte("x"))
	if len(coll) != 0 {
		t.Errorf("Wanted 3 elements")
	}
}

func TestCollection2(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer d.Close()
	d.Clear()
	d.SubSet([]byte("a"), []byte("b"), []byte("1"))
	d.SubSet([]byte("x"), []byte("b"), []byte("c"))
	d.SubSet([]byte("x"), []byte("c"), []byte("d"))
	d.SubSet([]byte("x"), []byte("d"), []byte("e"))
	d.Set([]byte("x"), []byte("b"))
	d.Set([]byte("z"), []byte("b"))
	coll := d.GetCollection([]byte("x"))
	if len(coll) != 3 {
		t.Fatalf("Wanted 3 elements, got %v", coll)
	}
	if string(coll[0].Key) != "b" || string(coll[0].Value) != "c" {
		t.Errorf("Wrong value")
	}
	if string(coll[1].Key) != "c" || string(coll[1].Value) != "d" {
		t.Errorf("Wrong value")
	}
	if string(coll[2].Key) != "d" || string(coll[2].Value) != "e" {
		t.Errorf("Wrong value")
	}
	d.SubClear([]byte("x"))
	coll = d.GetCollection([]byte("x"))
	if len(coll) != 0 {
		t.Errorf("Wanted 3 elements")
	}
}
