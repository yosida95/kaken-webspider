package main

import (
    "errors"
    "fmt"
    "log"
    "net/url"
    "sync"
)

var (
    CrawlerNotFound = errors.New("Crawler Not Found")
)

type Router struct {
    crawlers  map[string]*Crawler
    ring      *ConsistentHash
    sync.RWMutex
}

func NewRouter() *Router {
    r := new(Router)
    r.crawlers = make(map[string]*Crawler)
    r.ring = NewConsistentHash()

    return r
}

func (r *Router) Add(c *Crawler) {
    r.Lock()
    defer r.Unlock()

    r.crawlers[c.GetId()] = c
    r.ring.Add(c.GetId())
}

func (r *Router) Remove(c *Crawler) {
    r.Lock()
    defer r.Unlock()

    r.ring.Remove(c.GetId())
    delete(r.crawlers, c.GetId())
}

func (r *Router) Route(rawurl string) (c *Crawler, err error) {
    r.RLock()
    defer r.RUnlock()

    parsed, err := url.Parse(rawurl)
    if err != nil {
        log.Printf("Invalid URL: %s", err)
    }

    id, err := r.ring.Get(fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host))
    if err != nil {
        c = nil
        return
    }
    c = r.crawlers[id]
    return
}
