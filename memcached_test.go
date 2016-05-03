package gomemcached

import (
	"fmt"
	"testing"
)

func TestMemcached(t *testing.T) {

	t.Log("==init====")

	gc := New("127.0.0.1:11211")
	item := &Item{
		Key:        "dog",
		Value:      []byte("小狗"),
		Flags:      0,
		Exporation: 6000,
	}
	err := gc.Add(item)
	t.Log("err = ", err)

	item.Value = []byte("冬天")
	err = gc.Replace(item)

	t.Log("replace  err = ", err)

	item.Value = []byte("夏天")

	err = gc.Set(item)
	t.Log(" set err = ", err)
	it, err := gc.Get("dog")
	fmt.Println(string(it.Value), err)

	is := &Item{
		Key:   "num",
		Value: []byte("2"),
	}
	gc.Add(is)

	v, err := gc.Incr("num", 100)
	fmt.Println("v=", v, "err=", err)

}
