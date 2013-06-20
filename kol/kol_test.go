package kol

import (
	"bytes"
	"reflect"
	"testing"
)

type testStruct struct {
	Id   []byte
	Name string `kol:"index"`
	Age  int    `kol:"index"`
}

func TestCRUD(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	d.Clear()
	defer d.Close()
	mock := &testStruct{Id: []byte("hepp")}
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
	if hehu.Id == nil {
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
	if bytes.Compare(hehu2.Id, hehu.Id) != 0 {
		t.Errorf("Changed id")
	}
	hehu3 := testStruct{}
	if err := d.Get(hehu.Id, &hehu3); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(hehu2, hehu3) {
		t.Errorf("Did not get the same data")
	}
	if bytes.Compare(hehu3.Id, hehu.Id) != 0 {
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

func TestQuery(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	d.Clear()
	defer d.Close()
	hehu := testStruct{
		Name: "hehu",
		Age:  12,
	}
	if err := d.Set(&hehu); err != nil {
		t.Errorf(err.Error())
	}
	var res []testStruct
	if err := d.Query().All(&res); err != nil {
		t.Errorf(err.Error())
	}
	wanted := []testStruct{
		hehu,
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Errorf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Filter(Equals{"Name", "hehu"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Errorf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Filter(Equals{"Name", "blapp"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 0 {
		t.Errorf("Wanted [] but got %v", res)
	}
	res = nil
	if err := d.Query().Filter(And{Equals{"Name", "hehu"}, Equals{"Age", 12}}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Errorf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Filter(And{Equals{"Name", "blapp"}, Equals{"Age", 11}}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 0 {
		t.Errorf("Wanted [] but got %v", res)
	}
	res = nil
	if err := d.Query().Filter(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 12}, Equals{"Age", 11}}}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Errorf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Filter(And{Equals{"Name", "blapp"}, Or{Equals{"Age", 11}, Equals{"Age", 13}}}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 0 {
		t.Errorf("Wanted [] but got %v", res)
	}
}
