package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/dylanbr0wn/pbsb/gen/pbsb"
	"google.golang.org/protobuf/proto"
)

type Broker struct {
	Connections   *Connections
	Channels      *Channels
	Queue         *Queue
	ListnerLookup map[string]string
	listnerLock   sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		Connections:   &Connections{connections: make(map[string]*Connection)},
		ListnerLookup: make(map[string]string),
		listnerLock:   sync.RWMutex{},
		Channels:      &Channels{channels: make(map[string]*Channel)},
		Queue:         &Queue{BuffChan: make(chan *pbsb.Message, 2048)},
	}
}

func (b *Broker) AddConnection(conn net.Conn) {
	b.Connections.Add(conn)
}

func (b *Broker) RemoveConnection(conn net.Conn) {
	b.Connections.Remove(conn)
	b.listnerLock.Lock()

	if channel, ok := b.ListnerLookup[conn.RemoteAddr().String()]; ok {
		b.listnerLock.Unlock()
		b.RemoveListener(channel, conn.RemoteAddr().String())
	} else {
		b.listnerLock.Unlock()
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
		b.AddChannel(&Channel{Name: topic, Listeners: make([]string, 0)})
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
	if channel := b.Channels.Get(channel); channel != nil {
		buf, err := proto.Marshal(message)
		if err != nil {
			fmt.Printf("error marshaling message: %v\n", err)
			return err
		}
		msg := make([]byte, 4+len(buf))
		binary.BigEndian.PutUint32(msg, uint32(len(buf)))
		copy(msg[4:], buf)
		for _, listener := range channel.GetListeners() {
			if conn := b.Connections.Get(listener); conn != nil {
				err = conn.Send(msg)
				if err != nil {
					fmt.Printf("error writing message: %v\n", err)
					return err
				}
			}
		}

		return nil
	} else {
		return errors.New("channel does not exist")
	}
}

func (b *Broker) Listen(ctx context.Context) {
	for i := 0; i < 8; i++ {
		go func(ctx context.Context) {
		loop:
			for {
				select {
				case msg := <-b.Queue.Channel():
					channel := b.Channels.Get(msg.Topic)
					if channel != nil {
						go func() {
							buf, err := proto.Marshal(msg)
							if err != nil {
								fmt.Printf("error marshaling message: %v\n", err)
							}
							msgBytes := make([]byte, 4+len(buf))
							binary.BigEndian.PutUint32(msgBytes, uint32(len(buf)))
							copy(msgBytes[4:], buf)
							for _, listener := range channel.GetListeners() {
								if conn := b.Connections.Get(listener); conn != nil {
									if err = conn.Send(msgBytes); err != nil {
										fmt.Printf("error writing message: %v\n", err)
									}
								}
							}
						}()
					}
				case <-ctx.Done():
					break loop
				}

			}
		}(ctx)
	}
}

func (b *Broker) handleSubscribeRequest(conn net.Conn, subReq *pbsb.SubscribeRequest) error {
	if _, err := conn.Write([]byte{1}); err != nil {
		fmt.Printf("error writing response message: %v\n", err)
		return err
	}
	b.AddListener(subReq.Topic, conn.RemoteAddr().String())
	return nil
}

func (b *Broker) handlePublishRequest(conn net.Conn) error {
	if _, err := conn.Write([]byte{1}); err != nil {
		fmt.Printf("error writing response message: %v\n", err)
		return err
	}
	return nil
}

func (b *Broker) handleConnection(conn net.Conn) {
	fmt.Printf("new connection from: %s \n", conn.RemoteAddr().String())
	defer conn.Close()

	if _, err := conn.Write([]byte{1}); err != nil {
		fmt.Printf("error writing welcome message: %v\n", err)
		return
	}
	clientMessage := &pbsb.ClientMessage{}
	buf := make([]byte, 1024)
	for {

		n, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("client %s disconnected\n", conn.RemoteAddr().String())
			return
		}

		err = proto.Unmarshal(buf[:n], clientMessage)
		if err != nil {
			fmt.Printf("error unmarshaling client message: %v\n", err)
			return
		}
		switch clientMessage.Request.(type) {
		case *pbsb.ClientMessage_SubscribeRequest:
			req := clientMessage.Request.(*pbsb.ClientMessage_SubscribeRequest).SubscribeRequest
			if err := b.handleSubscribeRequest(conn, req); err != nil {
				fmt.Printf("error handling subscribe request: %v\n", err)
				return
			}
			continue
		case *pbsb.ClientMessage_Message:
			msg := clientMessage.Request.(*pbsb.ClientMessage_Message).Message
			b.Queue.Enqueue(msg)
			if err := b.handlePublishRequest(conn); err != nil {
				fmt.Printf("error handling publish request: %v\n", err)
				return
			}
		}
	}
}

func (b *Broker) Serve(port string) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		fmt.Printf("error listening: %v\n", err)
	}

	cleanup := func() {
		b.CloseConnections()
		listener.Close()
		fmt.Print("shutting down\n")
	}

	defer cleanup()

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cleanup()
		os.Exit(1)
	}()

	b.Listen(ctx)

	fmt.Print("listening on :8080\n")
	for {
		conn, err := listener.Accept()
		if err != nil {
			if err == net.ErrClosed {
				break
			}
			fmt.Printf("error accepting connection: %v\n", err)
			continue
		}
		b.AddConnection(conn)
		go func(conn net.Conn) {
			b.handleConnection(conn)
			b.RemoveConnection(conn)
		}(conn)
	}
}

func (b *Broker) CloseConnections() {
	b.Connections.lock.Lock()
	defer b.Connections.lock.Unlock()
	for _, conn := range b.Connections.connections {
		conn.Close()
	}
}
