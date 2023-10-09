package broker

import (
	"net"
	"sync"
)

type Connection struct {
	Conn net.Conn
	lock sync.RWMutex
}

func (c *Connection) Send(msg []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, err := c.Conn.Write(msg)
	return err
}

func (c *Connection) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.Conn.Close()
}

type Connections struct {
	connections map[string]*Connection
	lock        sync.RWMutex
}

func (c *Connections) Add(conn net.Conn) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.connections[conn.RemoteAddr().String()] = &Connection{Conn: conn}
}

func (c *Connections) Remove(conn net.Conn) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.connections, conn.RemoteAddr().String())
}

func (c *Connections) Get(addr string) *Connection {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if conn, ok := c.connections[addr]; ok {
		return conn
	} else {
		return nil
	}
}
