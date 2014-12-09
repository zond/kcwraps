package kol

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

type testStruct struct {
	Id        []byte
	Name      string `kol:"index"`
	Age       int    `kol:"index"`
	Email     string
	Dad       []byte `kol:"fk<Email>"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (self *testStruct) String() string {
	return fmt.Sprintf("%+v", *self)
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
	if err := d.Query().Where(Equals{"Name", "hehu"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Errorf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Where(Equals{"Name", "blapp"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 0 {
		t.Errorf("Wanted [] but got %v", res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "hehu"}, Equals{"Age", 12}}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Errorf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "blapp"}, Equals{"Age", 11}}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 0 {
		t.Errorf("Wanted [] but got %v", res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 12}, Equals{"Age", 11}}}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Errorf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "blapp"}, Or{Equals{"Age", 11}, Equals{"Age", 13}}}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 0 {
		t.Errorf("Wanted [] but got %v", res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 12}, Equals{"Age", 11}}}).Except(Equals{"Name", "blapp"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if !reflect.DeepEqual(res, wanted) {
		t.Errorf("Wanted %v but got %v", wanted, res)
	}
	res = nil
	if err := d.Query().Where(And{Equals{"Name", "blapp"}, Or{Equals{"Age", 11}, Equals{"Age", 13}}}).Except(Equals{"Name", "hehu"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 0 {
		t.Errorf("Wanted [] but got %v", res)
	}
	var res2 testStruct
	if found, err := d.Query().Where(And{Equals{"Name", "hehu"}, Or{Equals{"Age", 11}, Equals{"Age", 12}}}).Except(Equals{"Name", "blapp"}).First(&res2); err != nil || !found {
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
		where := strings.Split(string(debug.Stack()), "\n")[2]
		if !reflect.DeepEqual(rem, removed) {
			t.Errorf("%v: Wanted %v to have been deleted, but got %v", where, rem, removed)
		}
		if !reflect.DeepEqual(cre, created) {
			t.Errorf("%v: Wanted %v to have been created, but got %v", where, cre, created)
		}
		if !reflect.DeepEqual(upd, updated) {
			t.Errorf("%v: Wanted %v to have been updated, but got %v", where, upd, updated)
		}
	}
	done := make(chan bool)
	sub, err := d.Subscription("subtest1", &hehu, AllOps, func(obj interface{}, op Operation) error {
		switch op {
		case Delete:
			removed = append(removed, obj.(*testStruct))
		case Create:
			created = append(created, obj.(*testStruct))
		case Update:
			updated = append(updated, obj.(*testStruct))
		}
		done <- true
		return nil
	})
	if err != nil {
		t.Errorf(err.Error())
	}
	sub.Subscribe()
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
	sub, err := d.Query().Where(Equals{"Name", "qname"}).Subscription("subtest1", &hehu, AllOps, func(obj interface{}, op Operation) error {
		switch op {
		case Delete:
			removed = append(removed, obj.(*testStruct))
		case Create:
			created = append(created, obj.(*testStruct))
		case Update:
			updated = append(updated, obj.(*testStruct))
		}
		done <- true
		return nil
	})
	if err != nil {
		t.Errorf(err.Error())
	}
	sub.Subscribe()
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

type phase struct {
	Id   []byte
	Game []byte
}

func (self *phase) Updated(d *DB, old *phase) {
	g := game{Id: self.Game}
	if err := d.Get(&g); err != nil {
		panic(err)
	}
	d.EmitUpdate(&g)
}

type game struct {
	Id []byte
}

func (self *game) Updated(d *DB, old *game) {
	var members []member
	if err := d.Query().Where(Equals{"Game", self.Id}).All(&members); err != nil {
		panic(err)
	}
	for _, member := range members {
		cpy := member
		d.EmitUpdate(&cpy)
	}
}

type member struct {
	Id      []byte
	User    []byte
	Game    []byte `kol:"index"`
	updated chan bool
}

func (self *member) Updated(d *DB, old *member) {
	close(globalTestLock)
}

type user struct {
	Id []byte
}

var globalTestLock chan bool

func TestChains(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	d.Clear()
	defer d.Close()
	u := user{}
	if err := d.Set(&u); err != nil {
		t.Fatalf(err.Error())
	}
	g := game{}
	if err := d.Set(&g); err != nil {
		t.Fatalf(err.Error())
	}
	p := phase{Game: g.Id}
	if err := d.Set(&p); err != nil {
		t.Fatalf(err.Error())
	}
	m := member{Game: g.Id, User: u.Id}
	if err := d.Set(&m); err != nil {
		t.Fatalf(err.Error())
	}
	globalTestLock = make(chan bool)
	if err := d.Set(&p); err != nil {
		t.Fatalf(err.Error())
	}
	<-globalTestLock
}

func TestJoin(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	d.Clear()
	defer d.Close()
	dad := testStruct{
		Name: "dad",
		Age:  40,
	}
	if err := d.Set(&dad); err != nil {
		t.Fatalf(err.Error())
	}
	son := testStruct{
		Name:  "son",
		Age:   12,
		Email: "email",
		Dad:   dad.Id,
	}
	if err := d.Set(&son); err != nil {
		t.Fatalf(err.Error())
	}
	notdad := testStruct{
		Name: "notdad",
		Age:  41,
	}
	if err := d.Set(&notdad); err != nil {
		t.Fatalf(err.Error())
	}
	var res []testStruct
	if err := d.Query().Where(Join{&testStruct{Email: "email"}, "Email", "Dad"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 1 || bytes.Compare(res[0].Id, dad.Id) != 0 {
		t.Errorf("wanted %+v, got %+v", dad, res)
	}
	otherson := testStruct{
		Name:  "otherson",
		Age:   14,
		Email: "email",
		Dad:   dad.Id,
	}
	if err := d.Set(&otherson); err != nil {
		t.Fatalf(err.Error())
	}
	res = nil
	if err := d.Query().Where(Join{&testStruct{Email: "email"}, "Email", "Dad"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 1 || bytes.Compare(res[0].Id, dad.Id) != 0 {
		t.Errorf("wanted %+v, got %+v", dad, res)
	}
	if err := d.Del(&son); err != nil {
		t.Fatalf(err.Error())
	}
	res = nil
	if err := d.Query().Where(Join{&testStruct{Email: "email"}, "Email", "Dad"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 1 || bytes.Compare(res[0].Id, dad.Id) != 0 {
		t.Errorf("wanted %+v, got %+v", dad, res)
	}
	if err := d.Del(&otherson); err != nil {
		t.Fatalf(err.Error())
	}
	res = nil
	if err := d.Query().Where(Join{&testStruct{Email: "email"}, "Email", "Dad"}).All(&res); err != nil {
		t.Errorf(err.Error())
	}
	if len(res) != 0 {
		t.Errorf("wanted %+v, got %+v", nil, res)
	}
}

func isAlmost(t1, t2 time.Time) bool {
	if diff := t1.Sub(t2); diff > time.Second || diff < -time.Second {
		return false
	}
	return true
}

type ExampleStruct struct {
	Id             []byte
	SomeField      string
	SomeOtherField int
}

func ExampleCRUD() {
	// open the databse file "example" and panic if fail
	d := Must("example")
	// clear the database from previous example runs
	d.Clear()
	// create an example value without id
	exampleValue := &ExampleStruct{
		SomeField: "some value",
	}
	// put it in the database. this will give it an id
	if err := d.Set(exampleValue); err != nil {
		panic(err)
	}
	// create an empty value, but with an id
	loadedValue := &ExampleStruct{
		Id: exampleValue.Id,
	}
	// load it from the database. this will fill out the values using whatever is in the database with this id
	if err := d.Get(loadedValue); err != nil {
		panic(err)
	}
	fmt.Println(loadedValue.SomeField)
	// Output:
	// some value
}

func TestCreatedAt(t *testing.T) {
	d, err := New("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	d.Clear()
	defer d.Close()
	ts := &testStruct{}
	if err := d.Set(ts); err != nil {
		t.Errorf(err.Error())
	}
	if ts.CreatedAt.IsZero() {
		t.Errorf("Wanted non nil")
	}
	if ts.UpdatedAt.IsZero() {
		t.Errorf("Wanted non nil")
	}
	if !isAlmost(ts.UpdatedAt, ts.CreatedAt) {
		t.Errorf("Wanted equal")
	}
	oldUpd := ts.UpdatedAt
	oldCre := ts.CreatedAt
	ts.Name = "hehu"
	if err := d.Set(ts); err != nil {
		t.Errorf(err.Error())
	}
	if oldUpd.Equal(ts.UpdatedAt) {
		t.Errorf("Wanted non equal")
	}
	if !oldCre.Equal(ts.CreatedAt) {
		t.Errorf("Wanted equal")
	}
}
