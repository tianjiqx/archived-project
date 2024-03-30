package store

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/cfg"
	"github.com/stretchr/testify/assert"
)

// NewStore create store
func newStore(cfg cfg.StoreCfg) (Store, error) {

	// TODO: need to implement
	var peers = [][]string{{"http://127.0.0.1:22101"}, {"http://127.0.0.1:22201"}}
	return newPebbleStore(cfg, "127.0.0.1:8080", peers)
}

// go test -v   pkg/store/*.go
func TestBase(t *testing.T) {

	cfg := cfg.StoreCfg{Memory: false, DataPath: "/tmp/pebble"}
	c, er := newStore(cfg)

	assert.NoError(t, er, "TestSet new store failed")

	key := []byte("key")
	value := []byte("value")
	err := c.Set(key, value)
	assert.NoError(t, err, "TestSet failed")

	time.Sleep(2 * time.Second)

	v, err2 := c.Get(key)
	assert.NoError(t, err2, "TestGet failed")
	assert.Equal(t, string(value), string(v), "TestGet failed")

	// delete

	assert.NoError(t, c.Delete(key), "TestDelete failed")

	time.Sleep(time.Second)

	v, err = c.Get(key)
	assert.NoError(t, err, "TestDelete failed")
	assert.Empty(t, v, "TestDelete failed")

	time.Sleep(time.Second)

	err = c.Set(key, value)
	assert.NoError(t, err, "TestSet failed")

	time.Sleep(2 * time.Second)

	c.Close()

	/*
		//s, e := NewStore(cfg)
		s, e := newPebbleStore(cfg)
		v, err2 = s.Get(key)
		assert.NoError(t, err2, "TestGet failed")
		assert.Equal(t, string(value), string(v), "TestGet failed")
	*/

}

func TestGet(t *testing.T) {

	key := []byte("key")
	value := []byte("value")

	cfg := cfg.StoreCfg{Memory: false, DataPath: "/tmp/pebble"}
	s, e := newStore(cfg)
	assert.NoError(t, e, "TestGet new store failed")

	v, err2 := s.Get(key)
	assert.NoError(t, err2, "TestGet failed")
	assert.Equal(t, string(value), string(v), "TestGet failed")

	s.Close()
}

func set(s Store, i int, value []byte) {
	key := []byte(strconv.Itoa(i))

	s.Set(key, value)
}

// 1000  2.130361515s single replic
// 10000 4.128426092s ,2.105557187s ?
// 10000 2 region 10.908497213s
// 20000 2 regions 19.945779678s
func TestPerformace(t *testing.T) {

	cfg := cfg.StoreCfg{Memory: false, DataPath: "/tmp/pebble"}
	c, er := newStore(cfg)

	assert.NoError(t, er, "TestSet new store failed")

	i := 0
	value := []byte("value")
	start := time.Now()
	for i < 20000 {
		i = i + 1
		set(c, i, value)
	}
	//end := time.Now()
	//fmt.Printf("start: %v end: %v", start, end)

	tc := time.Since(start)
	time.Sleep(2 * time.Second)

	fmt.Printf("performace time cost = %v\n", tc)
	c.Close()
}
