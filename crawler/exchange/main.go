package main

import (
    "bufio"
    "flag"
    "fmt"
    "github.com/nu7hatch/gouuid"
    "io"
    "log"
    "net"
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

        for  {
            log.Printf("Waiting client")
            client, err := e.socket.Accept()
            log.Printf("Accepting client")
            if err != nil {
                log.Println(err)
                break
            }
            log.Printf("Connected from %s", client.RemoteAddr().String())

            go func() {
                crawler := NewCrawler(e.id, client)
                e.router.Add(crawler)
                defer e.router.Remove(crawler)

                reader := bufio.NewReader(crawler.GetConn())
                for {
                    url, err := reader.ReadString('\n')
                    if err != nil {
                        if err == io.EOF {
                            log.Printf("Connection closed by %s", client.RemoteAddr().String())
                        }else{
                            log.Println(err)
                        }
                        break
                    }
                    e.uchan <- url
                }
            }()
        }
    }()
    go func() {
        e.distributeUrl(uquit)
    }()

    <-quit
    <-uquit
}

func (e *Exchange) distributeUrl(quit chan<- bool) {
    for url := range e.uchan {
        crawler, err := e.router.Route(url)
        if err != nil {
            e.uchan <- url
            continue
        }

        eid := crawler.GetExchangeId()
        if eid == e.id {
            fmt.Fprintf(crawler.GetConn(), url)
        } else {
            // amqp.Publish(exchange), url)
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
            defer func(){
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
