package broker

import "sync"

type Channel struct {
	Name      string
	Listeners []string
	lock      sync.RWMutex
}

func (c *Channel) AddListener(listener string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.Listeners = append(c.Listeners, listener)
}

func (c *Channel) RemoveListener(listener string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for i, l := range c.Listeners {
		if l == listener {
			c.Listeners = append(c.Listeners[:i], c.Listeners[i+1:]...)
			break
		}
	}
}

func (c *Channel) GetListeners() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.Listeners
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
