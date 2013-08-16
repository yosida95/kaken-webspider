package exchange

import (
    "crypto/sha1"
    "github.com/stretchr/testify/assert"
    "io"
    "math/rand"
    "strconv"
    "testing"
)

func isNodeExists(c *ConsistentHash, token string) bool {
    var target nodeid
    for i := 0; i < c.numOfVnodes; i++ {
        key := c.hashKey(c.vnodeId(token, i))

        if _, exists := c.nodes[key]; !exists {
            return false
        }

        idx := c.search(key)
        if idx == 0 {
            target = c.ring[len(c.ring)-1]
        } else {
            target = c.ring[idx-1]
        }
        if compareNodeid(target, key) != 0 {
            return false
        }
    }

    return true

}

func TestNewConsistentHash(t *testing.T) {
    c := NewConsistentHash()
    assert.Equal(t, len(c.ring), 0)
    assert.Equal(t, len(c.nodes), 0)
    assert.Equal(t, c.numOfVnodes, 20)
}

func TestConsistentHashAdd(t *testing.T) {
    c := NewConsistentHash()

    c.Add("testnode")
    assert.Equal(t, len(c.ring), c.numOfVnodes)
    assert.Equal(t, len(c.nodes), c.numOfVnodes)

    assert.Equal(t, isNodeExists(c, "testnode"), true)
}

func TestConsistentHashRemove(t *testing.T) {
    c := NewConsistentHash()

    c.Add("node1")
    c.Add("node2")
    assert.Equal(t, isNodeExists(c, "node1"), true)
    assert.Equal(t, isNodeExists(c, "node2"), true)

    c.Remove("node2")
    assert.Equal(t, isNodeExists(c, "node1"), true)

    assert.Equal(t, len(c.ring), c.numOfVnodes)
    assert.Equal(t, len(c.nodes), c.numOfVnodes)
}

func TestConsistentHashGet(t *testing.T) {
    c := NewConsistentHash()

    _, err := c.Get("key")
    assert.Equal(t, err, EmptyError)

    c.Add("node1")
    c.Add("node2")

    key := c.hashKey("key")
    idx := c.search(key)

    node, err := c.Get("key")
    assert.Equal(t, node, c.nodes[c.ring[idx]])
    assert.Nil(t, err)
}

func TestConsistentHashhashKey(t *testing.T) {
    c := NewConsistentHash()

    var key nodeid
    h := sha1.New()
    for i := 0; i < 10; i++ {
        token := "key" + strconv.Itoa(rand.Int())
        io.WriteString(h, token)
        copy(key[:], h.Sum(nil))
        assert.Equal(t, compareNodeid(key, c.hashKey(token)), 0)

        h.Reset()
    }
}

func TestConsistentHashsearch(t *testing.T) {
    c := NewConsistentHash()

    c.Add("node1")
    c.Add("node2")
    c.Add("node3")

    for i := 0; i < 10; i++ {
        key := c.hashKey("key" + strconv.Itoa(rand.Int()))
        idx := c.search(key)

        assert.NotEqual(t, compareNodeid(c.ring[idx], key), -1)
        if idx > 0 {
            assert.Equal(t, compareNodeid(c.ring[idx-1], key), -1)
        }
    }
}

func TestConsistentHashupdateRing(t *testing.T) {
    c := NewConsistentHash()

    for i := 0; i < 100; i++ {
        key := "key" + strconv.Itoa(rand.Int())
        c.nodes[c.hashKey(key)] = key
    }
    c.updateRing()

    for i := 1; i < len(c.ring); i++ {
        assert.NotEqual(t, compareNodeid(c.ring[i-1], c.ring[i]), 1)
    }
}
