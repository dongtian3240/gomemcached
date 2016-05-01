package gomemcached

import (
	"testing"
)

func TestMemcached(t *testing.T) {

	t.Log("==init====")

	gc := New("127.0.0.1:11211")
	item := &Item{
		Key:        "dog",
		Value:      []byte("动态"),
		Flags:      0,
		Exporation: 60,
	}
	err := gc.Add(item)
	t.Log("err = ", err)

	item.Value = []byte("冬天")
	err = gc.Replace(item)

	t.Log("replace  err = ", err)

	item.Value = []byte("夏天")

	err = gc.Set(item)
	t.Log(" set err = ", err)
}
