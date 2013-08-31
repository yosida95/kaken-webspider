package main

import (
	"github.com/stretchr/testify/assert"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func TestCrawlQueuePush(t *testing.T) {
	q := NewCrawlQueue(1 * time.Second)
	u, err := url.Parse("http://example.com/")
	if !assert.Nil(t, err) {
		t.FailNow()
	}

	if !assert.Nil(t, q.Push(u)) || !assert.Equal(t, q.size, 1) {
		t.FailNow()
	}

	if got, err := q.Pop(); !assert.Nil(t, err) || !assert.Equal(t, got, u) {
		t.FailNow()
	}

	q.Close()
	if !assert.Equal(t, q.Push(u), QueueClosed) {
		t.FailNow()
	}
}

func TestCrawlQueuePop(t *testing.T) {
	q := NewCrawlQueue(10 * time.Millisecond)

	for i := 0; i < 10; i++ {
		u1, err := url.Parse("http://example.com/" + strconv.Itoa(i))
		if !assert.Nil(t, err) || !assert.Nil(t, q.Push(u1)) || !assert.Equal(t, q.size, 1) {
			t.FailNow()
		}
	}

	for i := 0; i < 15; i++ {
		u2, err := url.Parse("https://example.com/" + strconv.Itoa(i))
		if !assert.Nil(t, err) || !assert.Nil(t, q.Push(u2)) || !assert.Equal(t, q.size, 2) {
			t.FailNow()
		}
	}

	for i := 0; i < 20; i++ {
		var (
			u   *url.URL
			err error
		)

		if i%2 == 0 {
			u, err = url.Parse("http://example.com/" + strconv.Itoa(i/2))
		} else {
			u, err = url.Parse("https://example.com/" + strconv.Itoa(i/2))
		}

		if !assert.Nil(t, err) {
			t.FailNow()
		}

		got, err := q.Pop()
		if !assert.Nil(t, err) || !assert.Equal(t, got, u) {
			t.FailNow()
		}
	}

	for i := 10; i < 14; i++ {
		u, err := url.Parse("https://example.com/" + strconv.Itoa(i))
		if !assert.Nil(t, err) {
			t.FailNow()
		}

		got, err := q.Pop()
		if !assert.Nil(t, err) || !assert.Equal(t, got, u) {
			t.FailNow()
		}
	}

	uchan := make(chan *url.URL)
	go func() {
		u, err := url.Parse("https://example.com/" + strconv.Itoa(14))
		if !assert.Nil(t, err) {
			t.FailNow()
		}

		got, err := q.Pop()
		if !assert.Nil(t, err) || !assert.Equal(t, got, u) || !assert.Equal(t, q.size, 0) {
			t.FailNow()
		}

		uchan <- got
	}()

	select {
	case <-uchan:
		t.FailNow()
	case <-time.After(5 * time.Millisecond):
	}
}

func TestCrawlQueueFlush(t *testing.T) {
	q := NewCrawlQueue(1 * time.Second)
	if !assert.Equal(t, len(q.Flush()), 0) {
		t.FailNow()
	}

	for i := 0; i < 10; i++ {
		u1, err := url.Parse("http://example.com/" + strconv.Itoa(i))
		if !assert.Nil(t, err) || !assert.Nil(t, q.Push(u1)) || !assert.Equal(t, q.size, 1) {
			t.FailNow()
		}
	}

	for i := 0; i < 15; i++ {
		u2, err := url.Parse("https://example.com/" + strconv.Itoa(i))
		if !assert.Nil(t, err) || !assert.Nil(t, q.Push(u2)) || !assert.Equal(t, q.size, 2) {
			t.FailNow()
		}
	}

	got := q.Flush()
	if !assert.Equal(t, len(got), 25) {
		t.FailNow()
	}

	for i := 0; i < 10; i++ {
		u, err := url.Parse("http://example.com/" + strconv.Itoa(i))
		if !assert.Nil(t, err) || !assert.Equal(t, got[i], u) {
			t.FailNow()
		}
	}

	for i := 0; i < 15; i++ {
		u, err := url.Parse("https://example.com/" + strconv.Itoa(i))
		if !assert.Nil(t, err) || !assert.Equal(t, got[i+10], u) {
			t.FailNow()
		}
	}
}
