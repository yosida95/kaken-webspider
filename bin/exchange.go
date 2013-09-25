package main

import (
	"../exchange"
	"flag"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

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
			exchange := exchange.NewExchange(id, socket)
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
