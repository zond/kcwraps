package kc

import (
	"testing"
)

func TestCrud(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer d.Close()
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

func TestCollection(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer d.Close()
	d.SubSet([]byte("a"), []byte("b"), []byte("1"))
	d.SubSet([]byte("z"), []byte("b"), []byte("1"))
	d.SubSet([]byte("x"), []byte("b"), []byte("c"))
	d.SubSet([]byte("x"), []byte("c"), []byte("d"))
	d.SubSet([]byte("x"), []byte("d"), []byte("e"))
	coll := d.GetCollection([]byte("x"))
	if len(coll) != 3 {
		t.Errorf("Wanted 3 elements")
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
}
