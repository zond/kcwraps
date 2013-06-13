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
	if err := d.Set("k", "v"); err != nil {
		t.Errorf(err.Error())
	}
	if v, err := d.Get("k"); v != "v" || err != nil {
		t.Errorf("Wrong value")
	}
}

func TestCollections(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer d.Close()
	if err := d.SubSet("k1", "k2", "v"); err != nil {
		t.Errorf(err.Error())
	}
	if v, err := d.SubGet("k1", "k2"); v != "v" || err != nil {
		t.Errorf("Wrong value!")
	}
}
