package main

import (
	"github.com/nu7hatch/gouuid"
	"net"
)

type Crawler struct {
	id   string
	eid  exchangeid
	conn net.Conn
}

func NewCrawler(eid exchangeid, conn net.Conn) *Crawler {
	id, _ := uuid.NewV4()

	return &Crawler{id.String(), eid, conn}
}

func (c *Crawler) GetId() string {
	return c.id
}

func (c *Crawler) GetExchangeId() exchangeid {
	return c.eid
}

func (c *Crawler) GetConn() net.Conn {
	return c.conn
}
