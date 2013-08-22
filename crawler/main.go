package main

import (
	"crypto/sha1"
	"errors"
	"flag"
	"fmt"
	"github.com/tpjg/goriakpbc"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	RIAK_HOST   string = "RIAK_HOST:RIAK_PORT"
	RIAK_BUCKET string = "RIAK_BUCKET_NAME"
)

var (
	ManyRedirectErr = errors.New("Many Redirect Error")
	ERR_DATABASE    = errors.New("Database returned an error")
	ERR_DOWNLOAD    = errors.New("Failed to download a page")
	ERR_INTERNAL    = errors.New("Occur a internal error")
)

func SHA1Hash(token []byte) string {
	hash := sha1.New()
	hash.Write(token)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func main() {
	ipaddr := *flag.String("ip", "127.0.0.1", "IP address of exchange")
	port := *flag.Int("port", 9000, "Port of exchange")
	flag.Parse()

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ipaddr, port))
	if err != nil {
		log.Fatalf("Failed to connect to %s:%d", ipaddr, port)
	}

	riakClient := riak.New(RIAK_HOST)
	err = riakClient.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to Riak")
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

			crawler := NewCrawler(conn, riakClient)
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
