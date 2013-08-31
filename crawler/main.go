package main

import (
	"crypto/sha1"
	"errors"
	"flag"
	"fmt"
	"github.com/tpjg/goriakpbc"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const (
	RIAK_HOST    = "RIAK_HOST:RIAK_PORT"
	RIAK_BUCKET  = "RIAK_BUCKET"
	USER_AGENT   = "CRAWLER_USER_AGENT"
	CRAWLER_NAME = "CRAWLER_NAME_FOR_ROBOTS_TXT"
)

var (
	ERR_MANY_REDIRECT    = errors.New("Many Redirect Error")
	ERR_TIMEOUT          = errors.New("Request timed out")
	ERR_DATABASE         = errors.New("Database returned an error")
	ERR_DOWNLOAD         = errors.New("Failed to download a page")
	ERR_INTERNAL         = errors.New("Occur a internal error")
	ERR_INVALIDURL       = errors.New("URL is invalid")
	ERR_INVALID_ROBOTS   = errors.New("Robots.txt is invalid format")
	ERR_NOT_HTML         = errors.New("This page is not written in HTML")
	ERR_HTML_PARSE_ERROR = errors.New("Failed to parse HTML")
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

	riakClient := riak.New(RIAK_HOST)
	err := riakClient.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to Riak")
	}

	exchange := Exchange{
		ipaddr,
		port}
	crawler := NewCrawler(exchange, riakClient)
	go crawler.Start()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	select {
	case <-stop:
		log.Printf("Process is shutting down...")
		crawler.Stop()
	}
}
