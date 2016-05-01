package gomemcached

import (
	"errors"
	"hash/crc32"
	"net"
	"strings"
	"sync"
)

var (
	ErrNoServer = errors.New("not found server !")
)

type Selector interface {
	PickServer(key string) (net.Addr, error)
}

type GoServer struct {
	lk    sync.RWMutex
	addrs []net.Addr
}

func (gs *GoServer) PickServer(key string) (net.Addr, error) {
	gs.lk.RLock()
	defer gs.lk.RUnlock()
	if len(gs.addrs) == 0 {
		return nil, ErrNoServer
	}

	if len(gs.addrs) == 1 {
		return gs.addrs[0], nil
	}

	cs := crc32.ChecksumIEEE([]byte(key))

	return gs.addrs[cs%uint32(len(gs.addrs))], nil
}

func (gs *GoServer) SetServers(servers ...string) error {

	naddr := make([]net.Addr, len(servers))

	for i, server := range servers {

		if strings.Contains(server, "/") {
			addr, err := net.ResolveUnixAddr("unix", server)
			if err != nil {
				return err
			}

			naddr[i] = addr
		} else {
			addr, err := net.ResolveTCPAddr("tcp", server)

			if err != nil {
				return err
			}

			naddr[i] = addr
		}
	}

	gs.lk.Lock()
	defer gs.lk.Unlock()

	gs.addrs = naddr
	return nil
}
