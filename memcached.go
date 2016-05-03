package gomemcached

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"sync"
	"time"
)

const DEFAULT_CLIENT_TIMEOUT = time.Second * 5
const DEFAULT_FREE_GO_CONN_NUM = 5

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long, ASCII, and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")
)

var (
	CTRL              = []byte("\r\n")
	SPACE             = []byte(" ")
	RESULT_NOT_STORED = []byte("NOT_STORED\r\n")

	RESULT_STORED = []byte("STORED\r\n")

	RESULT_NOT_FOUND = []byte("NOT_FOUND\r\n")

	RESULT_END       = []byte("END\r\n")
	RESULT_DELETE_OK = []byte("DELETED\r\n")
)

type GoClient struct {
	goServer *GoServer
	lk       sync.Mutex
	connPool map[string][]*GoConn
	timeout  time.Duration
}

type GoConn struct {
	goClient *GoClient
	conn     net.Conn
	addr     net.Addr
	rw       *bufio.ReadWriter
}

type Item struct {
	Key        string
	Value      []byte
	Flags      uint32
	Exporation int32
}

func New(servers ...string) *GoClient {
	goServer := new(GoServer)
	goServer.SetServers(servers...)
	return &GoClient{
		goServer: goServer,
		timeout:  DEFAULT_CLIENT_TIMEOUT,
	}
}

func (gc *GoClient) putFreeGoConnToPool(goConn *GoConn) {

	gc.lk.Lock()
	defer gc.lk.Unlock()

	if gc.connPool == nil {
		gc.connPool = make(map[string][]*GoConn)
	}

	freelist := gc.connPool[goConn.addr.String()]
	if len(freelist) >= DEFAULT_FREE_GO_CONN_NUM {
		goConn.conn.Close()
		return
	}
	gc.connPool[goConn.addr.String()] = append(freelist, goConn)
}

func (gc *GoClient) getFreeGoConnFromPool(addr net.Addr) (*GoConn, bool) {

	gc.lk.Lock()
	defer gc.lk.Unlock()
	if gc.connPool == nil {
		return nil, false
	}

	freelist, ok := gc.connPool[addr.String()]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	gcConn := freelist[len(freelist)-1]

	gc.connPool[addr.String()] = freelist[:len(freelist)-1]
	return gcConn, true
}

func (gc *GoClient) dial(addr net.Addr) (net.Conn, error) {

	conn, err := net.DialTimeout(addr.Network(), addr.String(), gc.timeout)
	return conn, err

}
func (gc *GoClient) getGoConn(addr net.Addr) (*GoConn, error) {

	goConn, ok := gc.getFreeGoConnFromPool(addr)
	if ok {
		goConn.extendDeadline()
		return goConn, nil
	}

	conn, err := gc.dial(addr)

	if err != nil {
		return nil, err
	}

	goConn = &GoConn{
		goClient: gc,
		conn:     conn,
		addr:     addr,
		rw:       bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
	}

	goConn.extendDeadline()

	return goConn, nil

}

func (gcn *GoConn) release() {

	gcn.goClient.putFreeGoConnToPool(gcn)
}

func (gcn *GoConn) condRelease(err error) {
	if resumeableError(err) {
		gcn.goClient.putFreeGoConnToPool(gcn)
	} else {
		gcn.conn.Close()
	}
}
func (gcn *GoConn) extendDeadline() {
	gcn.conn.SetDeadline(time.Now().Add(gcn.goClient.timeout))
}

func (gc *GoClient) checkKey(key string) error {

	if len(key) > 250 {
		return ErrMalformedKey
	}

	for _, v := range key {

		if v <= ' ' || v >= 0x7f {
			return ErrMalformedKey
		}
	}
	return nil
}

func (gc *GoClient) onItem(item *Item, fn func(*bufio.ReadWriter, *Item) error) error {

	if gc.checkKey(item.Key) != nil {
		return ErrMalformedKey
	}
	addr, err := gc.goServer.PickServer(item.Key)

	if err != nil {
		return err
	}

	gconn, err := gc.getGoConn(addr)
	if err != nil {
		return err
	}

	defer gconn.condRelease(err)

	return fn(gconn.rw, item)

}
func (gc *GoClient) Add(item *Item) error {

	return gc.onItem(item, gc.add)
}

func (gc *GoClient) add(rw *bufio.ReadWriter, item *Item) error {

	return gc.parseStorePostAndResponse("add", rw, item)
}

func (gc *GoClient) Set(item *Item) error {

	return gc.onItem(item, gc.set)
}

func (gc *GoClient) set(rw *bufio.ReadWriter, item *Item) error {

	return gc.parseStorePostAndResponse("set", rw, item)
}

func (gc *GoClient) Replace(item *Item) error {

	return gc.onItem(item, gc.replace)
}

func (gc *GoClient) replace(rw *bufio.ReadWriter, item *Item) error {

	return gc.parseStorePostAndResponse("replace", rw, item)
}

func (gc *GoClient) parseStorePostAndResponse(command string, rw *bufio.ReadWriter, item *Item) error {

	_, err := fmt.Fprintf(rw, "%s %s %d %d %d%s", command, item.Key, item.Flags, item.Exporation, len(item.Value), CTRL)
	if err != nil {
		return err
	}

	_, err = rw.Write(item.Value)
	if err != nil {
		return err
	}

	_, err = rw.Write(CTRL)
	if err != nil {
		return err
	}
	err = rw.Flush()
	if err != nil {
		return err
	}

	line, err := rw.ReadSlice('\n')

	if err != nil {
		return err
	}
	fmt.Println(string(line))
	switch {

	case bytes.Equal(RESULT_STORED, line):
		return nil
	case bytes.Equal(RESULT_NOT_STORED, line):
		return ErrNotStored
	case bytes.Equal(RESULT_NOT_FOUND, line):
		return ErrCacheMiss

	}
	return fmt.Errorf("%s", string(line))

}

// give a key
func (gc *GoClient) Get(key string) (*Item, error) {

	gconn, err := gc.getGoConnWithKey(key)
	defer gconn.condRelease(err)
	return gc.parseGetResponse(gconn.rw, key)

}

func (gc *GoClient) getGoConnWithKey(key string) (*GoConn, error) {
	addr, err := gc.goServer.PickServer(key)
	if err != nil {
		return nil, err
	}

	gconn, err := gc.getGoConn(addr)
	if err != nil {
		return nil, err
	}
	return gconn, nil
}

func (gc *GoClient) parseGetResponse(rw *bufio.ReadWriter, key string) (*Item, error) {
	_, err := fmt.Fprintf(rw, "get %s%s", key, CTRL)
	if err != nil {
		return nil, err
	}

	err = rw.Flush()
	if err != nil {
		return nil, err
	}

	line, err := rw.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	fmt.Println("=result ===", string(line))

	var flags uint32
	var size uint32
	_, err = fmt.Sscanf(string(line), "VALUE %s %d %d\r\n", &key, &flags, &size)
	fmt.Printf("key = %s flags = %d size= %d", key, flags, size)

	vas, err := ioutil.ReadAll(io.LimitReader(rw.Reader, int64(size)+2))
	fmt.Println("vas =", string(vas))
	if err != nil {
		return nil, err
	}

	if !bytes.HasSuffix(vas, CTRL) {
		return nil, fmt.Errorf("error bad result ....")
	}

	val := vas[:size]
	item := &Item{
		Key:   key,
		Value: val,
		Flags: flags,
	}

	line, err = rw.ReadSlice('\n')

	if err != nil {
		return nil, err
	}

	fmt.Println("result =", string(line))

	return item, nil
}

func (gc *GoClient) responseScanLine(item *Item, line []byte) (int, error) {
	fmt.Println("=result ===", string(line))
	pattern := "VALUE %s %d %d\r\n"
	var key string
	var flags uint32
	var size uint32
	n, err := fmt.Sscanf(string(line), pattern, &key, &flags, &size)
	if err != nil {
		return 0, err
	}
	fmt.Printf("key = %s flags = %d size= %d", key, flags, size)
	item.Key = key
	item.Flags = flags

	return n, nil

}

/***
   delete
**/
func (gc *GoClient) Delete(key string) error {

	goconn, err := gc.getGoConnWithKey(key)
	if err != nil {
		return err
	}
	defer goconn.condRelease(err)

	_, err = fmt.Fprintf(goconn.rw, "delete %s %d\r\n", key, 0)
	if err != nil {
		return err
	}

	goconn.rw.Flush()

	line, err := goconn.rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(RESULT_DELETE_OK, line):
		return nil
	case bytes.Equal(RESULT_NOT_FOUND, line):
		return fmt.Errorf(string(RESULT_NOT_FOUND))
	}

	return fmt.Errorf(err.Error())

}

/***
  flush_all
**/
func (gc *GoClient) FlushAll() error {

	return gc.goServer.Each(func(addr net.Addr) error {

		goconn, err := gc.getGoConn(addr)
		if err != nil {
			return err
		}
		defer goconn.condRelease(err)

		_, err = goconn.rw.Write([]byte("flush_all"))

		if err != nil {
			return err
		}

		err = goconn.rw.Flush()
		if err != nil {
			return err
		}

		return nil

	})

}

/*
  incr
*/
func (gc *GoClient) Incr(key string, step uint32) (uint32, error) {

	goconn, err := gc.getGoConnWithKey(key)
	if err != nil {
		return 0, err
	}

	defer goconn.condRelease(err)
	return gc.responseIncrAndDecr(key, step, goconn.rw, "incr")

}

func (gc *GoClient) Decr(key string, step uint32) (uint32, error) {

	goconn, err := gc.getGoConnWithKey(key)
	if err != nil {
		return 0, err
	}

	defer goconn.condRelease(err)
	return gc.responseIncrAndDecr(key, step, goconn.rw, "decr")

}

func (gc *GoClient) responseIncrAndDecr(key string, step uint32, rw *bufio.ReadWriter, command string) (uint32, error) {

	_, err := fmt.Fprintf(rw, "%s %s %d\r\n", command, key, step)
	if err != nil {
		return 0, err

	}
	err = rw.Flush()
	if err != nil {
		return 0, err
	}

	line, err := rw.ReadSlice('\n')
	fmt.Println("incr line=", string(line))
	if err != nil {
		return 0, err
	}

	switch {
	case bytes.Equal(RESULT_NOT_FOUND, line):
		return 0, fmt.Errorf(string(RESULT_NOT_FOUND))

	}
	var res uint32
	_, err = fmt.Sscanf(string(line), "%d\r\n", &res)
	fmt.Println("res = ", res)
	if err != nil {
		return 0, err
	}
	return res, nil
}

/***
  resume error
**/
func resumeableError(err error) bool {
	if err == nil {
		return true
	}

	switch err {
	case ErrCacheMiss, ErrNotStored, ErrCASConflict, ErrMalformedKey:
		return true
	}
	return false
}
