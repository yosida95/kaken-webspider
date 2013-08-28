package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type exchangeid string

type Exchange struct {
	id     exchangeid
	router *Router
	socket net.Listener
	uchan  chan string
}

func NewExchange(id string, socket net.Listener) *Exchange {
	return &Exchange{exchangeid(id), NewRouter(), socket, make(chan string)}
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
	for rawurl := range e.uchan {
		crawler, err := e.router.Route(rawurl)
		if err != nil {
			log.Println(err)
			e.uchan <- rawurl
			continue
		}

		eid := crawler.GetExchangeId()
		if eid == e.id {
			fmt.Fprintf(crawler.GetConn(), rawurl)
		} else {
			// amqp.Publish(exchange), rawurl)
		}
	}
	quit <- true
}

func main() {
	newid, _ := uuid.NewV4()
	id := *flag.String("id", newid.String(), "Exchange ID")
	ip := *flag.String("ip", "0.0.0.0", "IP address for listen")
	port := *flag.Int("port", 9000, "Port number for listen")
	flag.Parse()

	isContinue := true
	for isContinue {
		func() {
			quit := make(chan bool, 1)
			quitted := make(chan bool, 1)
			defer func() {
				if err := recover(); err == nil {
					log.Println("Exiting...")
				} else {
					quit <- true
					log.Println(err)
					log.Println("Restarting...")
					time.Sleep(5 * time.Second)
					<-quitted
				}
			}()

			log.Printf("Listening %s:%d", ip, port)
			socket, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
			if err != nil {
				log.Fatalln(err)
				return
			}
			exchange := NewExchange(id, socket)
			go exchange.Start(quit, quitted)

			stop := make(chan os.Signal, 1)
			signal.Notify(stop, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGTERM)

			select {
			case <-quitted:
				isContinue = false
			case <-stop:
				quit <- true
				<-quitted
				isContinue = false
			}
		}()
	}
}
