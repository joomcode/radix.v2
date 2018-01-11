package pool

import (
	"sync"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
	"errors"
)

// Pool is a simple connection pool for redis Clients. It will create a small
// pool of initial connections, and if more connections are needed they will be
// created on demand. If a connection is Put back and the pool is full it will
// be closed.
type Pool struct {
	pool chan *redis.Client
	df   DialFunc

	stopOnce sync.Once
	stopCh   chan bool
}

// AddrFunc is a function which can be passing into NewCustom and used for
// populating pool with connection to different redis instances (e.g., slaves).
type AddrFunc func(idx int) string

func SingleAddrFunc(addr string) AddrFunc {
	return func(_ int) string { return addr }
}

// DialFunc is a function which can be passed into NewCustom
type DialFunc func(network, addr string) (*redis.Client, error)

// NewCustom is like New except you can specify a DialFunc which will be
// used when creating new connections for the pool. The common use-case is to do
// authentication for new connections.
func NewCustom(network string, size int, af AddrFunc, df DialFunc) (*Pool, error) {
	var client *redis.Client
	var err error
	pool := make([]*redis.Client, 0, size)
	for i := 0; i < size; i++ {
		addr := af(i)
		client, err = df(network, addr)
		if err != nil {
			for _, client = range pool {
				client.Close()
			}
			pool = pool[:0]
			break
		}
		pool = append(pool, client)
	}
	p := Pool{
		pool:    make(chan *redis.Client, len(pool)),
		df:      df,
		stopCh:  make(chan bool),
	}
	for i := range pool {
		p.pool <- pool[i]
	}

	if size < 1 {
		return nil, err
	}

	// set up a go-routine which will periodically ping connections in the pool.
	// if the pool is idle every connection will be hit once every 10 seconds.
	go func() {
		tick := time.NewTicker(10 * time.Second / time.Duration(size))
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				p.ping()
			case <-p.stopCh:
				return
			}
		}
	}()

	return &p, err
}

// New creates a new Pool whose connections are all created using
// redis.Dial(network, addr). The size indicates the maximum number of idle
// connections to have waiting to be used at any given moment. If an error is
// encountered an empty (but still usable) pool is returned alongside that error
func New(network, addr string, size int) (*Pool, error) {
	return NewCustom(network, size, SingleAddrFunc(addr), redis.Dial)
}

// Get retrieves an available redis client. If there are none available until
// the pool timeout, an error is returned.
func (p *Pool) Get() (*redis.Client, error) {
	select {
	case conn := <-p.pool:
		return conn, nil
	case <-time.After(time.Second * 5):
		return nil, errors.New("pool exhausted")
	}
}

// Put returns a client back to the pool. If the pool is full the client is
// closed instead. If the client is already closed (due to connection failure or
// what-have-you) it will not be put back in the pool
func (p *Pool) Put(conn *redis.Client) {
	select {
	case <-p.stopCh:
		conn.Close()
	default:
		if conn.LastCritical == nil {
			select {
			case p.pool <- conn:
			default:
				conn.Close()
			}
		} else {
			p.replenish(conn.Network, conn.Addr)
		}
	}
}

// Cmd automatically gets one client from the pool, executes the given command
// (returning its result), and puts the client back in the pool
func (p *Pool) Cmd(cmd string, args ...interface{}) *redis.Resp {
	c, err := p.Get()
	if err != nil {
		return redis.NewResp(err)
	}
	defer p.Put(c)

	return c.Cmd(cmd, args...)
}

// Close removes and calls Close() on all the connections currently in the pool.
// Assuming there are no other connections waiting to be Put back this method
// effectively closes and cleans up the pool.
func (p *Pool) Close() {
	p.stopOnce.Do(func() {
		close(p.stopCh)

		var conn *redis.Client
		for {
			select {
			case conn = <-p.pool:
				conn.Close()
			default:
				return
			}
		}
	})
}

// Avail returns the number of connections currently available to be gotten from
// the Pool using Get. If the number is zero then subsequent calls to Get will
// be creating new connections on the fly
func (p *Pool) Avail() int {
	return len(p.pool)
}

func (p *Pool) ping() {
	select {
	case conn := <-p.pool:
		defer p.Put(conn)
		conn.Cmd("PING")
	default:
	}
}

func (p *Pool) replenish(network, addr string) {
	go func() {
		for {
			conn, err := p.df(network, addr)
			if err == nil {
				p.Put(conn)
				return
			}

			select {
			case <-time.After(time.Second * 5):
				// continue
			case <-p.stopCh:
				return
			}
		}
	}()
}