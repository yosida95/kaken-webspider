package main

import (
    "log"
)


func (c *Crawler) startDownloader(quit chan<- bool) {
    for url := range c.cqueue {
        c.download(url)
    }
    quit <- true
}

func (c *Crawler) download(url string) {
    log.Println(url)
}
