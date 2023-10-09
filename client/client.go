package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"

	"github.com/dylanbr0wn/pbsb/gen/pbsb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client struct {
	Channel    string
	Connection net.Conn
	Queue      chan []byte
}

func NewClient() *Client {
	return &Client{
		Channel:    "",
		Connection: nil,
		Queue:      make(chan []byte, 2048),
	}
}

func (c *Client) Connect(brokerAddr string) error {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return err
	}
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err != nil {
		return err
	}
	res := buf[0]
	if res != byte(1) {
		return errors.New("invalid welcome message")
	}
	c.Connection = conn
	fmt.Printf("connected to broker with address: %s\n", brokerAddr)
	return nil
}

func (c *Client) Disconnect() error {
	return c.Connection.Close()
}

func (c *Client) Subscribe(channel string) error {
	subReq := pbsb.SubscribeRequest{
		Topic: channel,
	}

	clientMsg := pbsb.ClientMessage{
		Request: &pbsb.ClientMessage_SubscribeRequest{
			SubscribeRequest: &subReq,
		},
	}

	subReqBytes, err := proto.Marshal(&clientMsg)
	if err != nil {
		return err
	}
	if c.Connection == nil {
		return errors.New("not connected")
	}
	_, err = c.Connection.Write(subReqBytes)
	if err != nil {
		return err
	}
	buf := make([]byte, 1)
	_, err = c.Connection.Read(buf)
	if err != nil {
		return err
	}
	res := buf[0]
	if res != byte(1) {
		return errors.New("failed to subscribe to channel")
	}
	c.Channel = channel
	return nil
}

func (c *Client) Unsubscribe(channel string) error {
	if err := c.Disconnect(); err != nil {
		return err
	}
	c.Connection = nil
	c.Channel = ""
	return nil
}

func (c *Client) Publish(channel string, message string) error {
	msg := pbsb.Message{
		Topic:     channel,
		Message:   message,
		Timestamp: timestamppb.Now(),
	}

	clientMsg := pbsb.ClientMessage{
		Request: &pbsb.ClientMessage_Message{
			Message: &msg,
		},
	}

	msgBytes, err := proto.Marshal(&clientMsg)
	if err != nil {
		return err
	}
	if c.Connection == nil {
		return errors.New("not connected")
	}

	// c.Connection.SetWriteDeadline(time.Now().Add(5 * time.Millisecond))
	_, err = c.Connection.Write(msgBytes)
	if err != nil {
		return err
	}
	// log.Println("published message to channel:", channel)
	buf := make([]byte, 1)
	_, err = c.Connection.Read(buf)
	if err != nil {
		return err
	}
	res := buf[0]
	if res != byte(1) {
		return errors.New("failed to publish message")
	}
	// log.Println("Got response from broker")
	return nil
}

func (c *Client) Receive() ([]byte, error) {
	lenBuff := make([]byte, 4)
	_, err := c.Connection.Read(lenBuff)
	if err != nil {
		return nil, err
	}
	msgBuf := make([]byte, binary.BigEndian.Uint32(lenBuff))
	_, err = c.Connection.Read(msgBuf)
	if err != nil {
		return nil, err
	}

	return msgBuf, nil
}

func (c *Client) Listen() error {
	fmt.Printf("listening for messages on channel: %s\n", c.Channel)
	for i := 0; i < 8; i++ {
		go func() {
			bytes := <-c.Queue
			msg := &pbsb.Message{}
			if err := proto.Unmarshal(bytes, msg); err != nil {
				fmt.Printf("error unmarshaling message: %v\n", err)
				return
			}

			if msg.Topic == c.Channel {
				fmt.Printf("received message for topic %s, message: %s\n", msg.Topic, msg.Message)
			} else {
				fmt.Printf("received message for topic %s, but subscribed to %s\n", msg.Topic, c.Channel)
			}
		}()
	}
	for {
		bytes, err := c.Receive()
		if err != nil {
			fmt.Printf("error receiving message: %v\n", err)
			return err
		}
		c.Queue <- bytes
	}
}
