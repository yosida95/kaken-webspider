package main

import (
	"errors"
	"fmt"
	urlparse "net/url"
	"sort"
	"sync"
	"time"
)

var (
	QueueEmpty  = errors.New("Queue is empty")
	QueueClosed = errors.New("Queue was closed")
)

type QueueElement struct {
	url          urlparse.URL
	takeEffectAt time.Time
}

type CrawlQueue struct {
	queue    []*QueueElement
	leatest  map[string]*QueueElement
	size     int
	duration time.Duration
	closed   bool
	sync.Mutex
}

func NewCrawlQueue(duration time.Duration) *CrawlQueue {
	return &CrawlQueue{
		queue:    make([]*QueueElement, 0, 50),
		leatest:  make(map[string]*QueueElement),
		size:     0,
		duration: duration,
		closed:   false}
}

func (q CrawlQueue) Len() int {
	return q.size
}

func (q CrawlQueue) Less(i int, j int) bool {
	return q.queue[i].takeEffectAt.Before(q.queue[j].takeEffectAt)
}

func (q CrawlQueue) Swap(i int, j int) {
	q.queue[i], q.queue[j] = q.queue[j], q.queue[i]
}

func (q CrawlQueue) Push(url urlparse.URL) error {
	q.Lock()
	defer func() {
		q.Unlock()
	}()

	if q.closed {
		return QueueClosed
	}

	key := fmt.Sprintf("%s://%s", url.Scheme, url.Host)

	var element *QueueElement
	now := time.Now()
	if leatest, exists := q.leatest[key]; exists && leatest.takeEffectAt.After(now) {
		element = &QueueElement{url, leatest.takeEffectAt.Add(q.duration)}
	} else {
		element = &QueueElement{url, now}
	}

	q.size++
	if len(q.queue) < q.size {
		q.queue = append(q.queue, element)
	} else {
		q.queue[q.size-1] = element
		q.queue = q.queue[:q.size]
	}

	sort.Sort(q)
	return nil
}

func (q CrawlQueue) Pop() (urlparse.URL, error) {
	q.Lock()

	if q.size == 0 {
		q.Unlock()
		return urlparse.URL{}, QueueEmpty
	} else {
		element := q.queue[0]
		q.queue = q.queue[1:]
		q.size--
		q.Unlock()

		select {
		case <-time.After(element.takeEffectAt.Sub(time.Now())):
			return element.url, nil
		}
	}
}

func (q CrawlQueue) Join() {
	q.Lock()
	defer func() {
		q.Unlock()
	}()

	q.closed = true
}
