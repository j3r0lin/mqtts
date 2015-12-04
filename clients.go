package mqtt

import "sync"

type clients struct {
	sync.RWMutex
	m map[string]*client
}

func newClients() *clients {
	return &clients{
		m: make(map[string]*client),
	}
}

func (this *clients) add(c *client) (dulp bool) {
	this.Lock()
	defer this.Unlock()
	if p, ok := this.m[c.id]; ok {
		dulp = true
		go p.stop()
	}
	this.m[c.id] = c
	return
}

func (this *clients) delete(c *client) {
	this.Lock()
	defer this.Unlock()
	if p, ok := this.m[c.id]; ok && p == c {
		delete(this.m, c.id)
	}
}

func (this *clients) get(id string) (c *client, ok bool) {
	this.RLock()
	defer this.RUnlock()
	c, ok = this.m[id]
	return
}

func (this *clients) len() int {
	this.RLock()
	defer this.RUnlock()
	return len(this.m)
}
