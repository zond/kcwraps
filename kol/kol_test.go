package kol

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

type testStruct struct {
	Id   []byte
	Name string `kol:"index"`
	Age  int    `kol:"index"`
}

var benchdb *DB

type benchStruct1 struct {
	Id   []byte
	Name string
}

type benchStruct2 struct {
	Id   []byte
	Name string `kol:"index"`
}

func benchWrite(b *testing.B, size uint64) {
	var err error
	benchdb, err = New("bench")
	if err != nil {
		b.Fatalf(err.Error())
	}
	defer benchdb.Close()
	var s1 benchStruct1
	for count, _ := benchdb.Count(); count < size; count, _ = benchdb.Count() {
		s1.Id = nil
		s1.Name = fmt.Sprintf("%v%v", rand.Int63(), rand.Int63())
		if err := benchdb.Set(&s1); err != nil {
			b.Fatalf(err.Error())
		}
	}
	toAdd := make([]benchStruct1, b.N)
	for index, _ := range toAdd {
		toAdd[index] = benchStruct1{
			Name: fmt.Sprintf("%v%v", rand.Int63(), rand.Int63()),
		}
	}
	b.StartTimer()
	for _, s := range toAdd {
		benchdb.Set(&s)
	}
}

func BenchmarkWrite100(b *testing.B) {
	b.StopTimer()
	os.Remove("test.kct")
	os.Remove("test.kct.wal")
	benchWrite(b, 100)
}

func BenchmarkWrite1000(b *testing.B) {
	b.StopTimer()
	benchWrite(b, 1000)
}

func BenchmarkWrite10000(b *testing.B) {
	b.StopTimer()
	benchWrite(b, 10000)
}

func BenchmarkWrite100000(b *testing.B) {
	b.StopTimer()
	benchWrite(b, 100000)
}

func benchWriteIndex(b *testing.B, size uint64) {
	var err error
	benchdb, err = New("bench")
	if err != nil {
		b.Fatalf(err.Error())
	}
	defer benchdb.Close()
	var s1 benchStruct2
	for count, _ := benchdb.Count(); count < size; count, _ = benchdb.Count() {
		s1.Id = nil
		s1.Name = fmt.Sprintf("%v%v", rand.Int63(), rand.Int63())
		if err := benchdb.Set(&s1); err != nil {
			b.Fatalf(err.Error())
		}
	}
	toAdd := make([]benchStruct2, b.N)
	for index, _ := range toAdd {
		toAdd[index] = benchStruct2{
			Name: fmt.Sprintf("%v%v", rand.Int63(), rand.Int63()),
		}
	}
	b.StartTimer()
	for _, s := range toAdd {
		benchdb.Set(&s)
	}
}

func BenchmarkWriteIndex100(b *testing.B) {
	b.StopTimer()
	os.Remove("test.kct")
	os.Remove("test.kct.wal")
	benchWriteIndex(b, 100)
}

func BenchmarkWriteIndex1000(b *testing.B) {
	b.StopTimer()
	benchWriteIndex(b, 1000)
}

func BenchmarkWriteIndex10000(b *testing.B) {
	b.StopTimer()
	benchWriteIndex(b, 10000)
}

func BenchmarkWriteIndex100000(b *testing.B) {
	b.StopTimer()
	benchWriteIndex(b, 100000)
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
	hehu2 := testStruct{Id: hehu.Id}
	if err := d.Get(&hehu2); err != nil {
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
	hehu3 := testStruct{Id: hehu.Id}
	if err := d.Get(&hehu3); err != nil {
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
	hehu4 := testStruct{Id: hehu.Id}
	if err := d.Get(&hehu4); err != NotFound {
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
	res = nil
	if err := d.Query().Filter(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 12}, Equals{"Age", 11}}}).Except(Equals{"Name", "blapp"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Errorf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Filter(And{Equals{"Name", "blapp"}, Or{Equals{"Age", 11}, Equals{"Age", 13}}}).Except(Equals{"Name", "hehu"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 0 {
		t.Errorf("Wanted [] but got %v", res)
	}
	var res2 testStruct
	if err := d.Query().Filter(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 11}, Equals{"Age", 12}}}).Except(Equals{"Name", "blapp"}).First(&res2); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(hehu, res2) {
		t.Errorf("Wanted %v but got %v", hehu, res2)
	}
}

func TestIdSubscribe(t *testing.T) {
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
		t.Fatalf(err.Error())
	}
	var removed []*testStruct
	var created []*testStruct
	var updated []*testStruct
	var assertEvents = func(rem, cre, upd []*testStruct) {
		if !reflect.DeepEqual(rem, removed) {
			t.Errorf("Wanted %v to have been deleted, but got %v", rem, removed)
		}
		if !reflect.DeepEqual(cre, created) {
			t.Errorf("Wanted %v to have been created, but got %v", cre, created)
		}
		if !reflect.DeepEqual(upd, updated) {
			t.Errorf("Wanted %v to have been updated, but got %v", upd, updated)
		}
	}
	done := make(chan bool)
	if err := d.Subscribe("subtest1", &hehu, AllOps, func(obj interface{}, op Operation) {
		switch op {
		case Delete:
			removed = append(removed, obj.(*testStruct))
		case Create:
			created = append(created, obj.(*testStruct))
		case Update:
			updated = append(updated, obj.(*testStruct))
		}
		done <- true
	}); err != nil {
		t.Errorf(err.Error())
	}
	if err := d.Del(&hehu); err != nil {
		t.Errorf(err.Error())
	}
	<-done
	assertEvents([]*testStruct{&hehu}, nil, nil)
	hehu2 := hehu
	hehu2.Name = "blepp"
	if err := d.Set(&hehu2); err != nil {
		t.Errorf(err.Error())
	}
	<-done
	assertEvents([]*testStruct{&hehu}, []*testStruct{&hehu2}, nil)
	hehu3 := hehu2
	hehu3.Name = "jaja"
	if err := d.Set(&hehu3); err != nil {
		t.Errorf(err.Error())
	}
	<-done
	assertEvents([]*testStruct{&hehu}, []*testStruct{&hehu2}, []*testStruct{&hehu3})
	hehu4 := testStruct{
		Name: "knasen",
	}
	if err := d.Set(&hehu4); err != nil {
		t.Errorf(err.Error())
	}
	time.Sleep(time.Millisecond * 100)
	assertEvents([]*testStruct{&hehu}, []*testStruct{&hehu2}, []*testStruct{&hehu3})
}

func TestQuerySubscribe(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	d.Clear()
	defer d.Close()
	var removed []*testStruct
	var created []*testStruct
	var updated []*testStruct
	var assertEvents = func(rem, cre, upd []*testStruct) {
		if !reflect.DeepEqual(rem, removed) {
			t.Errorf("Wanted %v to have been deleted, but got %v", rem, removed)
		}
		if !reflect.DeepEqual(cre, created) {
			t.Errorf("Wanted %v to have been created, but got %v", cre, created)
		}
		if !reflect.DeepEqual(upd, updated) {
			t.Errorf("Wanted %v to have been updated, but got %v", upd, updated)
		}
	}
	done := make(chan bool)
	hehu := testStruct{}
	if err := d.Query().Filter(Equals{"Name", "qname"}).Subscribe("subtest1", &hehu, AllOps, func(obj interface{}, op Operation) {
		switch op {
		case Delete:
			removed = append(removed, obj.(*testStruct))
		case Create:
			created = append(created, obj.(*testStruct))
		case Update:
			updated = append(updated, obj.(*testStruct))
		}
		done <- true
	}); err != nil {
		t.Errorf(err.Error())
	}
	hehu.Name = "qname"
	if err := d.Set(&hehu); err != nil {
		t.Errorf(err.Error())
	}
	<-done
	assertEvents(nil, []*testStruct{&hehu}, nil)
	hehu2 := hehu
	hehu2.Age = 31
	if err := d.Set(&hehu2); err != nil {
		t.Errorf(err.Error())
	}
	<-done
	assertEvents(nil, []*testStruct{&hehu}, []*testStruct{&hehu2})
	if err := d.Del(&hehu2); err != nil {
		t.Errorf(err.Error())
	}
	<-done
	assertEvents([]*testStruct{&hehu2}, []*testStruct{&hehu}, []*testStruct{&hehu2})
	hehu3 := hehu2
	hehu3.Name = "othername"
	if err := d.Set(&hehu3); err != nil {
		t.Errorf(err.Error())
	}
	time.Sleep(time.Millisecond * 100)
	assertEvents([]*testStruct{&hehu2}, []*testStruct{&hehu}, []*testStruct{&hehu2})
}
