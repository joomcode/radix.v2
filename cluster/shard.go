package cluster

import (
	"github.com/mediocregopher/radix.v2/pool"
)

type shard struct {
	Pool  *pool.Pool
	Addrs shardAddrs
	NeedsHealthCheck bool
}

type shardAddrs []string

func newShardAddrs(addrs ...string) shardAddrs {
	return shardAddrs(addrs)
}

func (s shardAddrs) Master() string {
	if len(s) > 0 {
		return s[0]
	} else {
		return ""
	}
}

func (s shardAddrs) Slaves() []string {
	if len(s) > 1 {
		return s[1:]
	} else {
		return nil
	}
}

func (s shardAddrs) All() []string {
	return []string(s)
}

func (s shardAddrs) Equals(s2 shardAddrs) bool {
	if s.Master() != s2.Master() {
		return false
	}

	if len(s.Slaves()) != len(s2.Slaves()) {
		return false
	}

	for _, addr := range s.Slaves() {
		found := false
		for _, addr2 := range s2.Slaves() {
			if addr == addr2 {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}
