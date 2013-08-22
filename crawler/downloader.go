package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func (c *Crawler) startDownloader(quit chan<- bool) {
	knownUrlCache := make(map[string]bool)

	for url := range c.cqueue {
		urlhash := SHA1Hash([]byte(url))
		if knownUrlCache[urlhash] {
			log.Printf("%s has skipped because had crawled", url)
			continue
		} else if exists, err := c.pagestore.IsKnownURL(url); exists {
			log.Printf("%s has skipped because had crawled", url)
			continue
		} else if err != nil {
			log.Println(err)
			continue
		}

		page, redirectChain, err := c.download(url)
		if err != nil {
			log.Println(err)
			continue
		}

		c.pagestore.Save(page)
		for _, page := range redirectChain {
			c.pagestore.Save(page)
		}

		knownUrlCache[urlhash] = true
	}
	quit <- true
}

func (c *Crawler) download(url string) (p *Page, redirectChain []*Page, err error) {
	redirectChain = make([]*Page, 0)
	chkredirect := func(req *http.Request, via []*http.Request) error {
		if len(via) > 10 || req.URL.String() == via[len(via)-1].URL.String() {
			return ManyRedirectErr
		}
		page := NewPage(via[len(via)-1].URL.String(), 0, []byte{}, req.URL.String(), time.Now().UTC())
		redirectChain = append(redirectChain, page)
		return nil
	}
	client := &http.Client{CheckRedirect: chkredirect}

	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Add("User-Agent", "Mozilla/5.0 (compatible; yosida95-crawler/0.1; +https://kaken.yosida95.com/crawler.html)")
	response, err := client.Do(request)
	if err != nil {
		log.Println(err)
		err = ERR_DOWNLOAD
		return
	}

	body := []byte{}
	if response.StatusCode == 200 {
		defer response.Body.Close()
		if body, err = ioutil.ReadAll(response.Body); err != nil {
			log.Println(err)
			err = ERR_INTERNAL
			return
		}
	}

	p = NewPage(response.Request.URL.String(), response.StatusCode, body, "", time.Now().UTC())
	return
}
