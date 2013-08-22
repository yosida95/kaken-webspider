package main

import (
	"io/ioutil"
	"log"
	"net/http"
)

func (c *Crawler) startDownloader(quit chan<- bool) {
	knownUrlCache := make(map[string]bool)

	for url := range c.cqueue {
		urlhash := SHA1Hash([]byte(url))
		if knownUrlCache[urlhash] {
			continue
		} else if exists, err := c.pagestore.IsKnownURL(url); exists {
			continue
		} else if err != nil {
			log.Println(err)
		} else {
			c.download(url)
			knownUrlCache[urlhash] = true
		}
	}
	quit <- true
}

func (c *Crawler) download(url string) (*Page, error) {
	chkredirect := func(req *http.Request, via []*http.Request) error {
		if len(via) > 10 || req.URL.String() == via[len(via)-1].URL.String() {
			return ManyRedirectErr
		}
		c.pagestore.GetOrCreate(via[len(via)-1].URL.String(), 0, []byte{}, req.URL.String())
		return nil
	}
	client := &http.Client{CheckRedirect: chkredirect}

	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Add("User-Agent", "Mozilla/5.0 (compatible; yosida95-crawler/0.1; +https://kaken.yosida95.com/crawler.html)")
	response, err := client.Do(request)
	if err != nil {
		log.Println(err)
		return nil, ERR_DOWNLOAD
	}

	body := []byte{}
	if response.StatusCode == 200 {
		defer response.Body.Close()
		body, err = ioutil.ReadAll(response.Body)

		if err != nil {
			log.Println(err)
			return nil, ERR_INTERNAL
		}
	}

	return c.pagestore.GetOrCreate(response.Request.URL.String(), response.StatusCode, body, "")
}
