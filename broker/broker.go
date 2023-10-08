package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/dylanbr0wn/pbsb/gen/pbsb"
	"google.golang.org/protobuf/proto"
)

type Connections struct {
	connections map[string]net.Conn
	lock        sync.RWMutex
}

func (c *Connections) Add(conn net.Conn) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.connections[conn.RemoteAddr().String()] = conn
}

func (c *Connections) Remove(conn net.Conn) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.connections, conn.RemoteAddr().String())
}

type Channel struct {
	Name      string
	Listeners map[string]interface{}
	lock      sync.RWMutex
}

func (c *Channel) AddListener(listener string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.Listeners[listener] = nil
}

func (c *Channel) RemoveListener(listener string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.Listeners, listener)
}

type Channels struct {
	channels map[string]*Channel
	lock     sync.RWMutex
}

func (c *Channels) Add(channel *Channel) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.channels[channel.Name] = channel
}

func (c *Channels) Remove(channel *Channel) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.channels, channel.Name)
}

func (c *Channels) Get(name string) *Channel {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if channel, ok := c.channels[name]; ok {
		return channel
	} else {
		return nil
	}
}

type Broker struct {
	Connections   *Connections
	Channels      *Channels
	BuffChan      chan *pbsb.Message
	ListnerLookup map[string]string
	listnerLock   sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		Connections:   &Connections{connections: make(map[string]net.Conn)},
		ListnerLookup: make(map[string]string),
		listnerLock:   sync.RWMutex{},
		Channels:      &Channels{channels: make(map[string]*Channel)},
		BuffChan:      make(chan *pbsb.Message, 1024),
	}
}

func (b *Broker) AddConnection(conn net.Conn) {
	b.Connections.Add(conn)
}

func (b *Broker) RemoveConnection(conn net.Conn) {
	b.Connections.Remove(conn)
	b.listnerLock.Lock()
	defer b.listnerLock.Unlock()

	if channel, ok := b.ListnerLookup[conn.RemoteAddr().String()]; ok {
		b.listnerLock.Unlock()
		b.RemoveListener(channel, conn.RemoteAddr().String())
	}
}

func (b *Broker) AddChannel(channel *Channel) {
	b.Channels.Add(channel)
}

func (b *Broker) RemoveChannel(channel *Channel) {
	b.Channels.Remove(channel)
}

func (b *Broker) AddListener(topic, listener string) {
	if channel := b.Channels.Get(topic); channel != nil {
		channel.AddListener(listener)
	} else {
		b.AddChannel(&Channel{Name: topic, Listeners: make(map[string]interface{})})
		b.Channels.Get(topic).AddListener(listener)
	}
	b.listnerLock.Lock()
	defer b.listnerLock.Unlock()
	b.ListnerLookup[listener] = topic
}

func (b *Broker) RemoveListener(topic, listener string) error {
	if channel := b.Channels.Get(topic); channel != nil {
		channel.RemoveListener(listener)
		b.listnerLock.Lock()
		defer b.listnerLock.Unlock()
		delete(b.ListnerLookup, listener)
		return nil
	} else {
		return errors.New("channel does not exist")
	}
}

func (b *Broker) Broadcast(message *pbsb.Message) error {
	channel := message.Topic
	var wg sync.WaitGroup
	if channel := b.Channels.Get(channel); channel != nil {
		for listener := range channel.Listeners {
			if conn, ok := b.Connections.connections[listener]; ok {
				wg.Add(1)
				go func(conn net.Conn) {
					defer wg.Done()
					buf, err := proto.Marshal(message)
					if err != nil {
						log.Printf("error marshaling message: %v", err)
						return
					}
					_, err = conn.Write(buf)
					if err != nil {
						log.Printf("error writing message: %v", err)
						return
					}
				}(conn)
			}
		}
		wg.Wait()
		return nil
	} else {
		return errors.New("channel does not exist")
	}
}

func (b *Broker) Listen(ctx context.Context) {
loop:
	for {
		select {
		case msg := <-b.BuffChan:
			log.Println("broadcasting message:", msg.Message, "to channel", msg.Topic)
			if err := b.Broadcast(msg); err != nil {
				log.Printf("error broadcasting message: %v", err)
			}
		case <-ctx.Done():
			break loop
		}

	}
}

func (b *Broker) handleSubscribeRequest(conn net.Conn, subReq *pbsb.SubscribeRequest) error {

	subSuc := pbsb.SubscribeResponse{
		Success: true,
	}
	subSucBytes, err := proto.Marshal(&subSuc)
	if err != nil {
		log.Printf("error marshaling response message: %v", err)
		return err
	}
	if _, err := conn.Write(subSucBytes); err != nil {
		log.Printf("error writing response message: %v", err)
		return err
	}
	b.AddListener(subReq.Topic, conn.RemoteAddr().String())
	return nil
}

func (b *Broker) handlePublishRequest(conn net.Conn) error {
	pubSuc := pbsb.PublishResponse{
		Success: true,
	}
	pubSucBytes, err := proto.Marshal(&pubSuc)
	if err != nil {
		log.Printf("error marshaling response message: %v", err)
		return err
	}
	if _, err := conn.Write(pubSucBytes); err != nil {
		log.Printf("error writing response message: %v", err)
		return err
	}
	return nil
}

func (b *Broker) HandleConnection(conn net.Conn) {
	log.Println("new connection from:", conn.RemoteAddr().String())
	defer conn.Close()
	// begin onboard
	b.AddConnection(conn)
	defer b.RemoveConnection(conn)

	if _, err := conn.Write([]byte("welcome")); err != nil {
		log.Printf("error writing welcome message: %v", err)
		return
	}

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			b.RemoveConnection(conn)
			log.Printf("client disconnected")
			return
		}
		clientMessage := &pbsb.ClientMessage{}
		err = proto.Unmarshal(buf[:n], clientMessage)
		if err != nil {
			b.RemoveConnection(conn)
			log.Printf("error unmarshaling client message: %v", err)
			return
		}
		switch clientMessage.Request.(type) {
		case *pbsb.ClientMessage_SubscribeRequest:
			req := clientMessage.Request.(*pbsb.ClientMessage_SubscribeRequest).SubscribeRequest
			if err := b.handleSubscribeRequest(conn, req); err != nil {
				b.RemoveConnection(conn)
				log.Printf("error handling subscribe request: %v", err)
				return
			}
			continue
		case *pbsb.ClientMessage_Message:
			msg := clientMessage.Request.(*pbsb.ClientMessage_Message).Message
			b.BuffChan <- msg
			if err := b.handlePublishRequest(conn); err != nil {
				b.RemoveConnection(conn)
				log.Printf("error handling publish request: %v", err)
				return
			}
		}
	}
}

func (b *Broker) Serve(port string) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("error listening: %v", err)
	}

	cleanup := func() {
		b.CloseConnections()
		listener.Close()
		log.Println("shutting down")
	}

	defer cleanup()

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cleanup()
		os.Exit(1)
	}()

	go b.Listen(ctx)

	log.Println("listening on :8080")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error accepting connection: %v", err)
			continue
		}
		go b.HandleConnection(conn)
	}
}

func (b *Broker) CloseConnections() {
	b.Connections.lock.Lock()
	defer b.Connections.lock.Unlock()
	for _, conn := range b.Connections.connections {
		conn.Close()
	}
}
