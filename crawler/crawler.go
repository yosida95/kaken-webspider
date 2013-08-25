package main

import (
	"bufio"
	"github.com/tpjg/goriakpbc"
	"io"
	"log"
	"net"
)

type Crawler struct {
	conn      net.Conn
	cqueue    chan string // Crawl queue
	wqueue    chan string // Send to exchange queue
	pagestore *PageStore
}

func NewCrawler(conn net.Conn, riakClient *riak.Client) *Crawler {
	crawler := new(Crawler)
	crawler.conn = conn
	crawler.cqueue = make(chan string, 20)
	crawler.wqueue = make(chan string, 20)
	crawler.pagestore = NewPageStore(riakClient)

	return crawler
}

func (c *Crawler) Start(quit <-chan bool, quitted chan<- bool) {
	rquit := make(chan bool, 1)
	wquit := make(chan bool, 1)
	dquit := make(chan bool, 1)

	go c.startReader(rquit)
	go c.startWriter(wquit)
	go c.startDownloader(dquit)

	select {
	case <-quit:
		c.conn.Close()
		close(c.wqueue)
		<-wquit
		close(c.cqueue)
		<-dquit
	case <-rquit:
		close(c.wqueue)
		<-wquit
		close(c.cqueue)
		<-dquit
	case <-wquit:
		c.conn.Close()
		<-rquit
		close(c.cqueue)
		<-dquit
	}

	quitted <- true
}

func (c *Crawler) startReader(quit chan<- bool) {
	reader := bufio.NewReader(c.conn)
	for {
		url, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Printf("Connection closed by %s", c.conn.RemoteAddr().String())
			} else {
				log.Printf("Got error while reading URL: %v", err)
			}
			break
		}

		url = url[:len(url)-2]
		log.Printf("Got a URL: %s", url)
		c.cqueue <- url
	}

	quit <- true
}

func (c *Crawler) startWriter(quit chan<- bool) {
	for url := range c.wqueue {
		buf := []byte(url + "\n")
		for wrote := 0; wrote < len(buf); {
			if _wrote, err := c.conn.Write(buf[wrote:]); err == nil {
				wrote += _wrote
			} else {
				log.Printf("Got error while writing URL: %v", err)
				break
			}
		}

		log.Printf("Sent a URL: %s", url)
	}

	quit <- true
}
