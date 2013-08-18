package main

import (
    "bufio"
    "io"
    "log"
    "net"
)

type Crawler struct {
    conn   net.Conn
    cqueue chan string // Crawl queue
    wqueue chan string // Send to exchange queue
}

func NewCrawler(conn net.Conn) *Crawler {
    crawler := new(Crawler)
    crawler.conn = conn
    crawler.cqueue = make(chan string, 20)
    crawler.wqueue = make(chan string, 20)

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

        url = url[:len(url)-1]
        log.Printf("Got a URL: %s", url)
        c.cqueue <- url
    }

    quit <- true
}

func (c *Crawler) startWriter(quit chan<- bool) {
    writer := bufio.NewWriter(c.conn)
    for url := range c.wqueue {
        _, err := writer.WriteString(url)
        if err != nil {
            log.Printf("Got error while writing URL: %v", err)
            break
        }

        log.Printf("Sent a URL: %s", url)
    }

    quit <- true
}
