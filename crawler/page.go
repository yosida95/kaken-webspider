package main

import (
	"github.com/tpjg/goriakpbc"
	"log"
	urlparse "net/url"
	"time"
)

type CrawlingState struct {
	LastStatusCode int       `riak:"lastStatusCode"`
	LastDownload   time.Time `riak:"lastDownload"`
	Deleted        bool      `riak:"deleted"`
}

type Page struct {
	URL         string        `riak:"url"`
	ContentType string        `riak:"contentType"`
	Body        []byte        `riak:"body"`
	RedirectTo  string        `riak:"redirectTo"`
	State       CrawlingState `riak:"state"`
	riak.Model
}

func NewPage(url string, statusCode int, contentType string, body []byte, redirectTo string, downloadAt time.Time) *Page {
	state := CrawlingState{
		LastStatusCode: statusCode,
		LastDownload:   downloadAt,
		Deleted:        false}
	p := &Page{
		URL:         url,
		ContentType: contentType,
		Body:        body,
		RedirectTo:  redirectTo,
		State:       state}

	return p
}

type PageStore struct {
	client *riak.Client
}

func NewPageStore(client *riak.Client) *PageStore {
	return &PageStore{client}
}

func (s *PageStore) Get(url string) (*Page, error) {
	key := SHA1Hash([]byte(url))
	p := new(Page)

	if err := s.client.LoadModelFrom(RIAK_BUCKET, key, p); err == riak.NotFound {
		return nil, nil
	} else if err != nil {
		return nil, ERR_DATABASE
	} else {
		return p, nil
	}
}

func (s *PageStore) Save(p *Page) error {
	key := SHA1Hash([]byte(p.URL))

	if err := s.client.NewModelIn(RIAK_BUCKET, key, p); err != nil {
		log.Println(err)
		return ERR_DATABASE
	}

	if err := s.client.SaveAs(key, p); err != nil {
		log.Println(err)
		return ERR_DATABASE
	}

	log.Printf("%s has just been saved as %s", p.URL, key)
	return nil
}

func (s *PageStore) Delete(page *Page) {
	page.State.Deleted = true
	page.Save()
}

func (s *PageStore) IsKnownURL(url *urlparse.URL) (bool, error) {
	exists, err := s.client.ExistsIn(RIAK_BUCKET, SHA1Hash([]byte(url.String())))
	if err != nil {
		log.Println(err)
		return false, ERR_DATABASE
	}

	return exists, nil
}
