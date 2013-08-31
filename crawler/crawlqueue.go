package main

import (
	"errors"
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
	key          string
	url          *urlparse.URL
	takeEffectAt time.Time
	next         *QueueElement
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

func (q *CrawlQueue) Push(url *urlparse.URL) error {
	q.Lock()
	defer func() {
		q.Unlock()
	}()

	if q.closed {
		return QueueClosed
	}

	element := &QueueElement{url.Scheme + "://" + url.Host, url, time.Now(), nil}
	if leatest, exists := q.leatest[element.key]; exists {
		leatest.next = element
	} else {
		q.push(element)
	}

	q.leatest[element.key] = element
	return nil
}

func (q *CrawlQueue) Pop() (url *urlparse.URL, err error) {
	var element *QueueElement

	q.Lock()
	defer func() {
		q.Unlock()
		if err == nil {
			select {
			case <-time.After(element.takeEffectAt.Sub(time.Now())):
			}
		}
	}()

	if q.size == 0 {
		return &urlparse.URL{}, QueueEmpty
	}

	element = q.queue[0]
	if q.size == 1 {
		q.queue = q.queue[:0]
	} else {
		q.queue = q.queue[1:]
	}
	q.size--

	if next := element.next; next == nil {
		delete(q.leatest, element.key)
	} else {
		if now := time.Now(); element.takeEffectAt.Before(now) {
			next.takeEffectAt = now.Add(q.duration)
		} else {
			next.takeEffectAt = element.takeEffectAt.Add(q.duration)
		}
		q.push(next)
	}

	return element.url, nil
}

func (q *CrawlQueue) Flush() []*urlparse.URL {
	q.Lock()
	defer q.Unlock()

	urls := make([]*urlparse.URL, 0)
	for i := 0; i < q.size; i++ {
		for elem := q.queue[i]; elem != nil; elem = elem.next {
			urls = append(urls, elem.url)
		}
	}

	return urls
}

func (q *CrawlQueue) Close() {
	q.Lock()
	defer func() {
		q.Unlock()
	}()

	q.closed = true
}

func (q *CrawlQueue) push(elem *QueueElement) {
	q.size++
	q.queue = append(q.queue, elem)
	sort.Sort(q)
}
