package client

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGet3(t *testing.T) {
	c := New("127.0.0.1:8081")

	key := []byte("key2")
	value := []byte("value2")

	v, err := c.Get(key)
	assert.NoError(t, err, "TestGet failed")
	assert.Equal(t, string(value), string(v), "TestGet failed")

}

func TestSet(t *testing.T) {
	c := New("127.0.0.1:8081")

	key := []byte("key")
	value := []byte("value")
	err := c.Set(key, value)
	assert.NoError(t, err, "TestSet failed")
}

func TestGet(t *testing.T) {
	c := New("127.0.0.1:8081")
	time.Sleep(1 * time.Second)

	key := []byte("key")
	value := []byte("value")
	err := c.Set(key, value)
	assert.NoError(t, err, "TestGet failed")

	v, err := c.Get(key)
	assert.NoError(t, err, "TestGet failed")
	assert.Equal(t, string(value), string(v), "TestGet failed")
}

func TestGet2(t *testing.T) {
	c := New("127.0.0.1:8082")
	time.Sleep(1 * time.Second)

	key := []byte("key")
	value := []byte("value")
	err := c.Set(key, value)
	assert.NoError(t, err, "TestGet failed")

	v, err := c.Get(key)
	assert.NoError(t, err, "TestGet failed")
	assert.Equal(t, string(value), string(v), "TestGet failed")
}

func TestSet2(t *testing.T) {
	c := New("127.0.0.1:8082")

	key := []byte("key2")
	value := []byte("value2")
	err := c.Set(key, value)
	assert.NoError(t, err, "TestSet failed")

	time.Sleep(1 * time.Second)

	c = New("127.0.0.1:8081")

	v, _ := c.Get(key)
	assert.NoError(t, err, "TestGet failed")
	assert.Equal(t, string(value), string(v), "TestGet failed")

}

func TestDelete(t *testing.T) {
	c := New("127.0.0.1:8081")

	key := []byte("key")
	value := []byte("value")
	err := c.Set(key, value)
	assert.NoError(t, err, "TestDelete failed")

	time.Sleep(1 * time.Second)
	v, err := c.Get(key)
	assert.NoError(t, err, "TestDelete failed")
	assert.Equal(t, string(value), string(v), "TestDelete failed")

	assert.NoError(t, c.Delete(key), "TestDelete failed")

	time.Sleep(1 * time.Second)
	v, err = c.Get(key)
	assert.NoError(t, err, "TestDelete failed")
	assert.Empty(t, v, "TestDelete failed")
}

// singe region
// 1000 2.49s
// 20000 49.64100771s
// 20000 2 replics time cost = 36.112015682s
func TestPerformance(t *testing.T) {

	c := New("127.0.0.1:8081")

	i := 0
	value := []byte("value")
	start := time.Now()
	for i < 20000 {
		i = i + 1
		key := []byte(strconv.Itoa(i))
		c.Set(key, value)
	}
	//end := time.Now()
	//fmt.Printf("start: %v end: %v", start, end)

	tc := time.Since(start)
	fmt.Printf("time cost = %v\n", tc)

}

// test raft
func TestRaft(t *testing.T) {

	c := New("127.0.0.1:8081")

	i := 0
	value := []byte("value")
	start := time.Now()
	for {
		i = i + 1
		key := []byte(strconv.Itoa(i))
		c.Set(key, value)
	}
	//end := time.Now()
	//fmt.Printf("start: %v end: %v", start, end)

	tc := time.Since(start)
	fmt.Printf("time cost = %v\n", tc)

}
