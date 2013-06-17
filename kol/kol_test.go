package kol

import (
	"reflect"
	"testing"
)

type testStruct struct {
	Id   string
	Name string
	Age  int
}

func TestCRUD(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	d.Clear()
	defer d.Close()
	mock := &testStruct{Id: "hepp"}
	if err := d.Del(mock); err != NotFound {
		t.Errorf(err.Error())
	}
	hehu := testStruct{
		Name: "hehu",
		Age:  12,
	}
	if err := d.Set(&hehu); err != nil {
		t.Errorf(err.Error())
	}
	if hehu.Id == "" {
		t.Errorf("Did not create id")
	}
	hehu2 := testStruct{}
	if err := d.Get(hehu.Id, &hehu2); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(hehu, hehu2) {
		t.Errorf("Did not get the same data, wanted %+v but got %+v", hehu, hehu2)
	}
	hehu2.Age = 13
	if err := d.Set(&hehu2); err != nil {
		t.Errorf(err.Error())
	}
	if hehu2.Id != hehu.Id {
		t.Errorf("Changed id")
	}
	hehu3 := testStruct{}
	if err := d.Get(hehu.Id, &hehu3); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(hehu2, hehu3) {
		t.Errorf("Did not get the same data")
	}
	if hehu3.Id != hehu.Id {
		t.Errorf("Changed id")
	}
	if err := d.Del(&hehu); err != nil {
		t.Errorf(err.Error())
	}
	hehu4 := testStruct{}
	if err := d.Get(hehu.Id, &hehu4); err != NotFound {
		t.Errorf(err.Error())
	}
}
