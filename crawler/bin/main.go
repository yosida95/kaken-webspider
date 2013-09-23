package main

import (
	"../"
	"flag"
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

func main() {
	ipaddr := flag.String("ip", "127.0.0.1", "IP address of exchange")
	port := flag.Int("port", 9000, "Port of exchange")
	flag.Parse()

	riakClient := riak.New(RIAK_HOST)
	err := riakClient.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to Riak")
	}

	exchange := crawler.Exchange{
		*ipaddr,
		*port}
	crawler := crawler.NewCrawler(
		exchange,
		riakClient,
		RIAK_BUCKET,
		USER_AGENT,
		CRAWLER_NAME)
	go crawler.Start()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	select {
	case <-stop:
		log.Printf("Process is shutting down...")
		crawler.Stop()
	}
}
