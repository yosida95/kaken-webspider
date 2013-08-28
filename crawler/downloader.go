package main

import (
	"bytes"
	"code.google.com/p/go.net/html"
	"fmt"
	"github.com/temoto/robotstxt-go"
	"io/ioutil"
	"log"
	"net/http"
	urlparse "net/url"
	"strings"
	"time"
)

func (c *Crawler) startDownloader(quit chan bool) {
	knownUrlCache := make(map[string]bool)
	downloader := func(url *urlparse.URL) {
		urlString := url.String()

		if knownUrlCache[SHA1Hash([]byte(urlString))] {
			log.Printf("%s has skipped because had crawled", urlString)
		} else if exists, err := c.pagestore.IsKnownURL(url); err != nil {
			log.Printf("%s has skipped because an error occurred: %v", urlString, err)
		} else if exists {
			log.Printf("%s has skipped because had crawled", urlString)
		} else {
			if c.checkRobotsPolicy(url) {
				page, redirectChain, err := c.download(url)
				if err != nil {
					log.Println(err)
				} else {
					knownUrlCache[SHA1Hash([]byte(page.URL))] = true

					if urls, err := c.detectURLs(page); err == nil {
						for _, url := range urls {
							log.Printf("Detected URL: %s", urlString)
							c.wqueue <- url
						}
					}

					c.pagestore.Save(page)
					for _, page := range redirectChain {
						c.pagestore.Save(page)
						knownUrlCache[SHA1Hash([]byte(page.URL))] = true
					}
				}
			} else {
				log.Printf("%s has skipped because denied crawling by robots.txt", url.String())
			}
		}
	}

	urlchan := make(chan *urlparse.URL, 10)
	go func() {
		for {
			url, err := c.cqueue.Pop()
			if err == QueueEmpty {
				time.Sleep(1 * time.Second)
			} else {
				urlchan <- url
			}
		}
	}()

loop:
	for {
		select {
		case <-quit:
			break loop
		case url := <-urlchan:
			downloader(url)
			continue
		}
	}

	quit <- true
	log.Printf("Stopped downloader")
}

func (c *Crawler) download(url *urlparse.URL) (p *Page, redirectChain []*Page, err error) {
	redirectChain = make([]*Page, 0)
	chkredirect := func(req *http.Request, via []*http.Request) error {
		if len(via) > 10 || req.URL.String() == via[len(via)-1].URL.String() {
			return ManyRedirectErr
		}
		page := NewPage(via[len(via)-1].URL.String(), 0, "", []byte{}, req.URL.String(), time.Now().UTC())
		redirectChain = append(redirectChain, page)
		return nil
	}
	client := &http.Client{CheckRedirect: chkredirect}

	request := &http.Request{
		Method:     "GET",
		URL:        url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Body:       nil,
		Host:       url.Host,
	}
	request.Header.Add("User-Agent", USER_AGENT)

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

	p = NewPage(response.Request.URL.String(), response.StatusCode, response.Header.Get("Content-Type"), body, "", time.Now().UTC())
	return
}

func (c *Crawler) checkRobotsPolicy(url *urlparse.URL) bool {
	robotstxtURL := &urlparse.URL{
		Scheme: url.Scheme,
		User:   url.User,
		Host:   url.Host,
		Path:   "/robots.txt"}

	var robotstxtData *Page
	var err error
	if robotstxtData, err = c.pagestore.Get(robotstxtURL.String()); err != nil {
		return true
	} else if robotstxtData == nil {
		robotstxtData, _, err = c.download(robotstxtURL)
		if err != nil {
			log.Printf("Error occurred during downloading robots.txt: %v", err)
			return true
		} else if robotstxtData == nil {
			return true
		}

		if robotstxtData.URL != robotstxtURL.String() {
			// redirected
			return true
		} else {
			if err = c.pagestore.Save(robotstxtData); err != nil {
				log.Printf("Error occurred during saving robots.txt: %v", err)
				return true
			}
		}
	}

	if robotstxtData.State.LastStatusCode != 200 {
		return true
	}

	robots, err := robotstxt.FromBytes(robotstxtData.Body)
	if err != nil {
		log.Println("Error occurred during parsing robots.tx: %v", err)
		return true
	}

	robotsGroup := robots.FindGroup(CRAWLER_NAME)
	if url.RawQuery == "" {
		return robotsGroup.Test(url.Path)
	} else {
		return robotsGroup.Test(fmt.Sprintf("%s?%s", url.Path, url.RawQuery))
	}
}

func (c *Crawler) detectURLs(p *Page) ([]*urlparse.URL, error) {
	if !strings.HasPrefix(p.ContentType, "text/html") && !strings.HasPrefix(p.ContentType, "application/xhtml+xml") {
		return nil, ERR_NOT_HTML
	}

	doc, err := html.Parse(bytes.NewReader(p.Body))
	if err != nil {
		log.Printf("Failed to parse HTML of %s via %v", p.URL, err)
		return nil, ERR_HTML_PARSE_ERROR
	}

	// FIXME invalid url
	base, _ := urlparse.Parse(p.URL)
	URLs := make(map[string]bool)
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" && !URLs[attr.Val] {
					URLs[attr.Val] = true
					break
				}
			}
		} else if n.Type == html.ElementNode && n.Data == "base" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					_base, err := urlparse.Parse(attr.Val)
					if err != nil || _base.IsAbs() {
						log.Printf("Invalid URL: %s", attr.Val)
					}
					base = _base
				}
			}
		}

		for child := n.FirstChild; child != nil; child = child.NextSibling {
			f(child)
		}
	}
	f(doc)

	result := make([]*urlparse.URL, 0, len(URLs))
	for _url := range URLs {
		if url, err := urlparse.Parse(_url); err == nil {
			if !url.IsAbs() {
				url.Scheme = base.Scheme
				url.Host = url.Host

				if URLs[url.String()] {
					continue
				}
			}

			result = append(result, url)
		} else {
			log.Printf("Invalid URL: %s", _url)
		}
	}

	return result, nil
}
