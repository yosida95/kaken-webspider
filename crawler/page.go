package main

import (
	"github.com/tpjg/goriakpbc"
	"log"
	"time"
)

type CrawlingState struct {
	LastStatusCode int       `riak:"lastStatusCode"`
	LastDownload   time.Time `riak:"lastDownload"`
	Deleted        bool      `riak:"deleted"`
}

type Page struct {
	URL        string        `riak:"url"`
	Body       []byte        `riak:"body"`
	RedirectTo string        `riak:"redirectTo"`
	State      CrawlingState `riak:"state"`
	riak.Model
}

type PageStore struct {
	client *riak.Client
}

func NewPageStore(client *riak.Client) *PageStore {
	return &PageStore{client}
}

func (s *PageStore) GetOrCreate(url string, statusCode int, body []byte, redirectTo string) (*Page, error) {
	p := &Page{}
	now := time.Now()
	urlhash := SHA1Hash([]byte(url))

	if exists, err := s.IsKnownURL(url); err != nil {
		return nil, err
	} else if exists {
		if err := s.client.LoadModelFrom(RIAK_BUCKET, urlhash, p); err != nil {
			log.Println(err)
			return nil, ERR_DATABASE
		}
	} else {
		if err := s.client.NewModelIn(RIAK_BUCKET, urlhash, p); err != nil {
			log.Println(err)
			return nil, ERR_DATABASE
		}
	}

	p.URL = url
	p.Body = body
	p.RedirectTo = redirectTo
	p.State = CrawlingState{statusCode, now, false}

	if err := p.Save(); err != nil {
		log.Println(err)
		return nil, ERR_DATABASE
	}

	log.Printf("%s has just been saved as %s", url, urlhash)
	return p, nil
}

func (s *PageStore) Delete(page *Page) {
	page.State.Deleted = true
	page.Save()
}

func (s *PageStore) IsKnownURL(url string) (bool, error) {
	exists, err := s.client.ExistsIn(RIAK_BUCKET, SHA1Hash([]byte(url)))
	if err != nil {
		log.Println(err)
		return false, ERR_DATABASE
	}

	return exists, nil
}
