package main

import (
	"bytes"
	"code.google.com/p/go.net/html"
	"github.com/temoto/robotstxt-go"
	"io/ioutil"
	"log"
	"net/http"
	urlparse "net/url"
	"strings"
	"time"
)

func (c *Crawler) startDownloader(quit chan<- bool) {
	knownUrlCache := make(map[string]bool)

	for url := range c.cqueue {
		if knownUrlCache[SHA1Hash([]byte(url))] {
			log.Printf("%s has skipped because had crawled", url)
		} else if exists, err := c.pagestore.IsKnownURL(url); err != nil {
			log.Printf("%s has skipped because an error occurred: %v", url, err)
		} else if exists {
			log.Printf("%s has skipped because had crawled", url)
		} else {
			allowed, err := c.checkRobotsPolicy(url)
			if err != nil {
				log.Printf("%s has skipped because an error occurred: %v", url, err)
			}

			if allowed {
				page, redirectChain, err := c.download(url)
				if err != nil {
					log.Println(err)
				} else {
					knownUrlCache[SHA1Hash([]byte(page.URL))] = true

					if urls, err := c.detectURLs(page); err == nil {
						for _, url := range urls {
							log.Printf("Detected URL: %s", url)
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
				log.Printf("%s has skipped because denied crawling by robots.txt", url)
			}
		}
	}
	quit <- true
}

func (c *Crawler) download(url string) (p *Page, redirectChain []*Page, err error) {
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

	request, _ := http.NewRequest("GET", url, nil)
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

func (c *Crawler) checkRobotsPolicy(url string) (bool, error) {
	parsed, err := urlparse.Parse(url)
	if err != nil {
		log.Println(err)
		return false, ERR_INVALIDURL
	}

	robotstxtURL := &urlparse.URL{Scheme: parsed.Scheme, User: parsed.User, Host: parsed.Host, Path: "/robots.txt"}

	var robotstxtData *Page
	if robotstxtData, err = c.pagestore.Get(robotstxtURL.String()); err != nil {
		return true, err
	} else if robotstxtData == nil {
		robotstxtData, _, err = c.download(robotstxtURL.String())
		if err != nil {
			return true, err
		}

		err = c.pagestore.Save(robotstxtData)
		if err != nil {
			return true, err
		}
	}

	if robotstxtData.State.LastStatusCode != 200 {
		return true, nil
	}

	robots, err := robotstxt.FromBytes(robotstxtData.Body)
	if err != nil {
		log.Println(err)
		return true, ERR_INVALID_ROBOTS
	}

	robotsGroup := robots.FindGroup(CRAWLER_NAME)
	return robotsGroup.Test(parsed.Path), nil
}

func (c *Crawler) detectURLs(p *Page) ([]string, error) {
	if !strings.HasPrefix(p.ContentType, "text/html") && !strings.HasPrefix(p.ContentType, "application/xhtml+xml") {
		return nil, ERR_NOT_HTML
	}

	doc, err := html.Parse(bytes.NewReader(p.Body))
	if err != nil {
		log.Printf("Failed to parse HTML of %s via %v", p.URL, err)
		return nil, ERR_HTML_PARSE_ERROR
	}

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

	result := make([]string, 0, len(URLs))
	for _url := range URLs {
		if url, err := urlparse.Parse(_url); err == nil {
			if !url.IsAbs() {
				url.Scheme = base.Scheme
				url.Host = url.Host

				if URLs[url.String()] {
					continue
				}
			}

			result = append(result, url.String())
		} else {
			log.Printf("Invalid URL: %s", _url)
		}
	}

	return result, nil
}
