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
	if err := d.SubSet("k1", "k2", "v"); err != nil {
		t.Errorf(err.Error())
	}
	if v, err := d.SubGet("k1", "k2"); v != "v" || err != nil {
		t.Errorf("Wrong value!")
	}
	if err := d.SubRemove("k1", "k2"); err != nil {
		t.Errorf(err.Error())
	}
	if v, err := d.SubGet("k1", "k2"); v == "v" || err == nil {
		t.Errorf("Not removed!")
	}

	if err := d.SubSetInt("k1", "k4", 33); err != nil {
		t.Errorf(err.Error())
	}
	if v2, err := d.SubGetInt("k1", "k4"); v2 != 33 || err != nil {
		t.Errorf("Wrong value!")
	}
	if err := d.SubRemove("k1", "k4"); err != nil {
		t.Errorf(err.Error())
	}
	if v2, err := d.SubGetInt("k1", "k4"); v2 == 33 || err == nil {
		t.Errorf("Not removed!")
	}

	if err := d.SubSetGob("k1", "k3", "v2"); err != nil {
		t.Errorf(err.Error())
	}
	var v string
	if err := d.SubGetGob("k1", "k3", &v); v != "v2" || err != nil {
		t.Errorf("Wrong value!")
	}
	if err := d.SubRemove("k1", "k3"); err != nil {
		t.Errorf(err.Error())
	}
	if err := d.SubGetGob("k1", "k3", &v); v == "v" || err == nil {
		t.Errorf("Not removed!")
	}
}
