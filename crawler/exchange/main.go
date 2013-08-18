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
    "syscall"
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

func (e *Exchange) Start(quit <-chan bool) {
    uquit := make(chan bool, 1)

    go func() {
        defer func() {
            e.socket.Close()
            close(e.uchan)
        }()

        for {
            log.Printf("Waiting client")
            client, err := e.socket.Accept()
            log.Printf("Accepting client")
            if err != nil {
                log.Println(err)
                break
            }
            log.Printf("Connected from %s", client.RemoteAddr().String())
            go func() {
                e.handleConnection(client)
            }()
        }
    }()
    go func() {
        e.distributeUrl(uquit)
    }()

    <-quit
    <-uquit
}

func (e *Exchange) handleConnection(client net.Conn) {
    crawler := NewCrawler(e.id, client)
    e.router.Add(crawler)
    defer e.router.Remove(crawler)

    reader := bufio.NewReader(crawler.GetConn())
    for {
        rawurl, err := reader.ReadString('\n')
        if err != nil {
            if err == io.EOF {
                log.Printf("Connection closed by %s", client.RemoteAddr().String())
            } else {
                log.Println(err)
            }
            break
        }

        switch parsed, err := url.Parse(rawurl); {
        case len(rawurl) < 1:
            log.Printf("Invalid URL: %s", rawurl)
        case err != nil:
            log.Printf("Invalid URL: %s (%v)", rawurl, err)
        case parsed.Scheme != "http" && parsed.Scheme != "https":
            log.Printf("Invalid URL: %s", rawurl)
        default:
            e.uchan <- rawurl[:len(rawurl)-2]
        }
    }
}

func (e *Exchange) distributeUrl(quit chan<- bool) {
    for rawurl := range e.uchan {
        crawler, err := e.router.Route(rawurl)
        if err != nil {
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
    for isContinue == true {
        func() {
            quit := make(chan bool, 1)
            defer func() {
                quit <- true
                if err := recover(); err == nil {
                    isContinue = false
                    log.Println("Exixting...")
                }
            }()

            log.Printf("Listening %s:%d", ip, port)
            socket, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
            if err != nil {
                log.Fatalln(err)
                return
            }
            exchange := NewExchange(id, socket)
            go exchange.Start(quit)

            stop := make(chan os.Signal, 1)
            signal.Notify(stop, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGTERM)
            <-stop
        }()
    }
}