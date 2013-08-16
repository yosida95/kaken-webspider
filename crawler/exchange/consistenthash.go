package exchange

import (
    "crypto/sha1"
    "errors"
    "io"
    "sort"
    "strconv"
    // "log"
    "sync"
)

var (
    EmptyError = errors.New("No node have been added")
)

type nodeid [20]byte
type nodering []nodeid

func (x nodering) Len() int {
    return len(x)
}

func (x nodering) Less(i int, j int) bool {
    return compareNodeid(x[i], x[j]) == -1
}

func (x nodering) Swap(i int, j int) {
    x[i], x[j] = x[j], x[i]
}

func compareNodeid(x nodeid, y nodeid) int {
    for i := 0; i < 20; i++ {
        if x[i] > y[i] {
            return 1
        } else if x[i] < y[i] {
            return -1
        }
    }
    return 0
}

type ConsistentHash struct {
    ring        nodering
    nodes       map[nodeid]string
    numOfVnodes int
    sync.RWMutex
}

func NewConsistentHash() *ConsistentHash {
    c := new(ConsistentHash)
    c.numOfVnodes = 20
    c.nodes = make(map[nodeid]string)

    return c
}

func (c *ConsistentHash) Add(token string) {
    c.Lock()
    defer c.Unlock()

    for i := 0; i < c.numOfVnodes; i++ {
        c.nodes[c.hashKey(c.vnodeId(token, i))] = token
    }

    c.updateRing()
}

func (c *ConsistentHash) Remove(token string) {
    c.Lock()
    defer c.Unlock()

    for i := 0; i < c.numOfVnodes; i++ {
        delete(c.nodes, c.hashKey(c.vnodeId(token, i)))
    }

    c.updateRing()
}

func (c *ConsistentHash) Get(token string) (string, error) {
    c.RLock()
    defer c.RUnlock()

    if len(c.ring) == 0 {
        return "", EmptyError
    }

    i := c.search(c.hashKey(token))
    return c.nodes[c.ring[i]], nil
}

func (c *ConsistentHash) vnodeId(token string, i int) string {
    return token + "$" + strconv.Itoa(i)
}

func (c *ConsistentHash) hashKey(token string) (key nodeid) {
    h := sha1.New()
    io.WriteString(h, token)
    copy(key[:], h.Sum(nil))

    return
}

func (c *ConsistentHash) search(key nodeid) (i int) {
    i = sort.Search(len(c.ring), func(i int) bool {
        return compareNodeid(c.ring[i], key) == 1
    })

    if i >= len(c.ring) {
        i = 0
    }
    return
}

func (c *ConsistentHash) updateRing() {
    ring := c.ring[:0]
    for key := range c.nodes {
        ring = append(ring, key)
    }

    sort.Sort(ring)
    c.ring = ring
}
