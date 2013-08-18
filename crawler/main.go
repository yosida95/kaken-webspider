package main

import (
    "flag"
    "fmt"
    "log"
    "net"
    "os"
    "os/signal"
    "syscall"
    "time"
)


func main() {
    ipaddr := *flag.String("ip", "127.0.0.1", "IP address of exchange")
    port := *flag.Int("port", 9000, "Port of exchange")
    flag.Parse()

    conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ipaddr, port))
    if err != nil {
        log.Fatalf("Faild to connect to %s:%d", ipaddr, port)
    }

    isContinue := true
    for isContinue {
        func() {
            quit := make(chan bool, 1)
            quitted := make(chan bool, 1)

            defer func() {
                if err := recover(); err == nil {
                    log.Printf("Exiting...")
                    isContinue = false
                } else {
                    log.Println(err)
                    log.Printf("Restarting...")
                    time.Sleep(5 * time.Second)
                }
            }()

            crawler := NewCrawler(conn)
            go crawler.Start(quit, quitted)

            stop := make(chan os.Signal, 1)
            signal.Notify(stop, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGTERM)

            select {
            case <-quitted:
            case <-stop:
                quit <- true
                <-quitted
            }
        }()
    }
}
