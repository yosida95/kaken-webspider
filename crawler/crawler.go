package main

import (
	"bufio"
	"fmt"
	"github.com/tpjg/goriakpbc"
	"io"
	"log"
	"net"
	urlparse "net/url"
	"time"
)

type Exchange struct {
	IPAddr string
	Port   int
}

type Crawler struct {
	exchange  Exchange
	cqueue    *CrawlQueue        // Crawl queue
	wqueue    chan *urlparse.URL // Send to exchange queue
	pagestore *PageStore
}

func NewCrawler(exchange Exchange, riakClient *riak.Client) *Crawler {
	crawler := new(Crawler)
	crawler.exchange = exchange
	crawler.cqueue = NewCrawlQueue(5 * time.Second) // sleep crawling to same netloc for 5 seconds
	crawler.wqueue = make(chan *urlparse.URL, 20)
	crawler.pagestore = NewPageStore(riakClient)

	return crawler
}

func (c *Crawler) joinExchange(quit chan bool) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", c.exchange.IPAddr, c.exchange.Port))
	if err != nil {
		quit <- true
		return
	}
	defer conn.Close()

	rquit := make(chan bool, 2)
	go c.reader(conn, rquit)
	defer close(rquit)

	wquit := make(chan bool, 2)
	go c.writer(conn, wquit)
	defer close(wquit)

	select {
	case <-quit:
		log.Printf("Stopping reader")
		rquit <- true
		time.Sleep(1 * time.Second)
		<-rquit

		log.Printf("Stopping writer")
		wquit <- true
		time.Sleep(1 * time.Second)
		<-wquit
	case <-rquit:
		log.Printf("Stopping writer")
		wquit <- true
		time.Sleep(1 * time.Second)
		<-wquit
	case <-wquit:
		log.Printf("Stopping reader")
		rquit <- true
		time.Sleep(1 * time.Second)
		<-rquit

	}

	quit <- true
	log.Printf("Shut down connection with exchange")
}

func (c *Crawler) reader(conn net.Conn, quit chan bool) {
	urlchan := make(chan string, 20)

	pushURL := func(urlString string) error {
		url, err := urlparse.Parse(urlString)
		if err != nil {
			log.Printf("URL parsing error: %v", err)
			return err
		}

		c.cqueue.Push(url)
		return nil
	}

	_quit := make(chan bool, 1)
	go func() {
		var urlString string
		var err error

		defer func() {
			if err := recover(); err != nil {
				// residual URL
				if url, err := urlparse.Parse(urlString); err == nil {
					c.wqueue <- url
				}

				log.Printf("Recovered panic: %v", err)
			}
		}()

		reader := bufio.NewReader(conn)
		for {
			urlString, err = reader.ReadString('\n')
			if err == io.EOF {
				log.Printf("Connection closed by exchange")
				break
			} else if err != nil {
				log.Printf("An error occurred: %v", err)
				break
			}

			urlchan <- urlString[:len(urlString)-1]
		}

		_quit <- true
	}()

loop:
	for {
		select {
		case <-_quit:
			break loop
		case <-quit:
			break loop
		case urlString := <-urlchan:
			pushURL(urlString)
		}
	}

	close(urlchan)
	for urlString := range urlchan {
		pushURL(urlString)
	}

	quit <- true
	log.Printf("Stopeed reader")
}

func (c *Crawler) writer(conn net.Conn, quit chan bool) {
	writer := func(body []byte) error {
		for wrote := 0; wrote < len(body); {
			if _wrote, err := conn.Write(body[wrote:]); err == nil {
				wrote += _wrote
			} else {
				return err
			}
		}
		return nil
	}

	writeURL := func(url *urlparse.URL) error {
		err := writer([]byte(url.String() + "\n"))
		if err != nil {
			log.Printf("Got error while writing URL: %v", err)
		}
		return err
	}

loop:
	for {
		select {
		case url := <-c.wqueue:
			if writeURL(url) != nil {
				c.wqueue <- url
				break loop
			}
		case <-quit:
			if err := writer([]byte("QUIT\n")); err == nil {
				log.Printf("Sent quitting message")
			} else {
				log.Printf("Error occurred during sending quitting message: %v", err)
				break loop
			}

		childLoop:
			for {
				select {
				case url := <-c.wqueue:
					if writeURL(url) != nil {
						c.wqueue <- url
						break childLoop
					}
				case <-time.After(1 * time.Second):
					// c.wqueue is empty
					break childLoop
				}
			}

			break loop
		}
	}

	quit <- true
	log.Printf("Stopped writer")
}

func (c *Crawler) Start(quit chan bool) {
	equit := make(chan bool, 2)
	defer close(equit)

	dquit := make(chan bool, 1)
	defer close(dquit)

	go c.joinExchange(equit)
	go c.startDownloader(dquit)

loop:
	for {
		select {
		case <-quit:
			log.Printf("Stopping downloader")
			dquit <- true
			time.Sleep(1 * time.Second)
			<-dquit

			log.Printf("Leaving from exchange")
			equit <- true
			time.Sleep(1 * time.Second)
			<-equit

			break loop
		case <-equit:
			log.Printf("connection with exchange is down")
			time.Sleep(1 * time.Second)
			log.Printf("Rejoin to exchange")
			go c.joinExchange(equit)
		}
	}

	quit <- true
}
