package client

import (
	"errors"
	"log"
	"net"

	"github.com/dylanbr0wn/pbsb/gen/pbsb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client struct {
	Channel    string
	Connection net.Conn
}

func NewClient() *Client {
	return &Client{
		Channel:    "",
		Connection: nil,
	}
}

func (c *Client) Connect(brokerAddr string) error {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return err
	}
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	if string(buf[:n]) != "welcome" {
		return errors.New("invalid welcome message")
	}
	c.Connection = conn
	log.Println("connected to broker with address:", brokerAddr)
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
	log.Println("Waiting for response...")
	buf := make([]byte, 1024)
	n, err := c.Connection.Read(buf)
	if err != nil {
		return err
	}
	res := &pbsb.SubscribeResponse{}
	if proto.Unmarshal(buf[:n], res); err != nil {
		return errors.New("invalid response from broker")
	}
	if !res.Success {
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

	_, err = c.Connection.Write(msgBytes)
	if err != nil {
		return err
	}
	log.Println("published message to channel:", channel)
	buf := make([]byte, 1024)
	n, err := c.Connection.Read(buf)
	if err != nil {
		return err
	}
	res := &pbsb.PublishResponse{}
	if proto.Unmarshal(buf[:n], res); err != nil {
		return errors.New("invalid response from broker")
	}
	if !res.Success {
		return errors.New("failed to publish to channel")
	}
	log.Println("Got response from broker")
	return nil
}

func (c *Client) Receive() (*pbsb.Message, error) {
	buf := make([]byte, 1024)
	n, err := c.Connection.Read(buf)
	if err != nil {
		return nil, err
	}
	msg := &pbsb.Message{}
	if err = proto.Unmarshal(buf[:n], msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *Client) Listen() error {
	log.Println("listening for messages on channel:", c.Channel)
	for {
		msg, err := c.Receive()
		if err != nil {
			return err
		}
		if msg.Topic == c.Channel {
			println(msg.Message)
		} else {
			log.Printf("received message for topic %s, but subscribed to %s", msg.Topic, c.Channel)
		}
	}
}
