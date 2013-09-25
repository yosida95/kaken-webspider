package exchange

import (
	"bufio"
	"io"
	"log"
	"net"
	"net/url"
	"strings"
)

type exchangeid string

type Exchange struct {
	id     exchangeid
	router *Router
	socket net.Listener
	uchan  chan string
}

func NewExchange(id string, socket net.Listener) *Exchange {
	return &Exchange{
		exchangeid(id),
		NewRouter(),
		socket,
		make(chan string)}
}

func (e *Exchange) Start(quit <-chan bool, quitted chan<- bool) {
	squit := make(chan bool, 1)
	uquit := make(chan bool, 1)

	go e.waitClient(squit)
	go e.distributeUrl(uquit)

	select {
	case <-quit:
		e.socket.Close()
		<-squit
	case <-squit:
	}

	close(e.uchan)
	<-uquit

	quitted <- true
}

func (e *Exchange) waitClient(quit chan<- bool) {
	for {
		log.Printf("Waiting client")
		client, err := e.socket.Accept()
		log.Printf("Accepting client")
		if err != nil {
			log.Println(err)
			break
		}
		log.Printf("Connected from %s", client.RemoteAddr().String())

		go e.handleConnection(client)
	}

	quit <- true
}

func (e *Exchange) handleConnection(client net.Conn) {
	defer client.Close()

	crawler := NewCrawler(e.id, client)
	e.router.Add(crawler)

	reader := bufio.NewReader(crawler.GetConn())
	for {
		rawurl, err := reader.ReadString('\n')
		if err == io.EOF {
			log.Printf("Connection closed by %s", client.RemoteAddr().String())

			e.router.Remove(crawler)
			break
		} else if err != nil {
			log.Println(err)

			e.router.Remove(crawler)
			break
		}

		if strings.HasSuffix(rawurl, "\r\n") {
			rawurl = rawurl[:len(rawurl)-2] + "\n"
		}
		if rawurl == "QUIT\n" {
			e.router.Remove(crawler)
			continue
		}

		switch parsed, err := url.Parse(rawurl[:len(rawurl)-1]); {
		case err != nil:
			log.Printf("Invalid URL: %s (%v)", rawurl, err)
		case parsed.Scheme != "http" && parsed.Scheme != "https":
			log.Printf("Invalid URL: %s", rawurl)
		default:
			e.uchan <- rawurl

			log.Printf("Got a URL from %s: %s", client.RemoteAddr().String(), rawurl)
		}
	}
}

func (e *Exchange) distributeUrl(quit chan<- bool) {
	writer := func(conn net.Conn, body []byte) error {
		for wrote := 0; wrote < len(body); {
			if _wrote, err := conn.Write(body[wrote:]); err == nil {
				wrote += _wrote
			} else {
				return err
			}
		}
		return nil
	}

	for rawurl := range e.uchan {
		crawler, err := e.router.Route(rawurl)
		if err != nil {
			log.Println(err)
			e.uchan <- rawurl
			continue
		}

		eid := crawler.GetExchangeId()
		if eid == e.id {
			writer(crawler.GetConn(), []byte(rawurl))
		} else {
			/* TODO
			amqp.Publish(exchange), rawurl)
			*/
		}
	}
	quit <- true
}
