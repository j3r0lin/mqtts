package mqtt

import (
	"container/list"
	"errors"
	"strings"
	"sync"
)

type subscribe struct {
	client *client
	qos    byte
}

type subscribes struct {
	sync.RWMutex
	subs map[string]*subscribe
}

func newSubscribes() *subscribes {
	return &subscribes{subs: make(map[string]*subscribe)}
}

func (this *subscribes) add(client *client, qos byte) {
	this.Lock()
	defer this.Unlock()
	if sub, ok := this.subs[client.id]; ok {
		sub.qos = qos
	} else {
		this.subs[client.id] = &subscribe{client, qos}
	}
}

func (this *subscribes) remove(client *client) {
	this.Lock()
	defer this.Unlock()
	for _, sub := range this.subs {
		if sub.client.id == client.id {
			delete(this.subs, client.id)
			break
		}
	}
}

func (this *subscribes) len() int {
	this.RLock()
	defer this.RUnlock()
	return len(this.subs)
}

func (this *subscribes) forEach(callback func(*client, byte)) {
	this.RLock()
	defer this.RUnlock()
	for _, sub := range this.subs {
		go callback(sub.client, sub.qos)
	}
}

type subhier struct {
	sync.RWMutex
	routes map[string]*subhier
	subs   *subscribes
}

func newSubhier() *subhier {
	return &subhier{
		routes: make(map[string]*subhier),
		subs:   newSubscribes(),
	}
}

func (this *subhier) subscribe(topic string, cli *client, qos byte) error {
	if tokens, err := topicTokenise(topic); err != nil {
		return err
	} else {
		return this.processSubscribe(tokens, cli, qos)
	}
}

func (this *subhier) unsubscribe(topic string, cli *client) error {
	if tokens, err := topicTokenise(topic); err != nil {
		return err
	} else {
		return this.processUnSubscribe(tokens, cli)
	}
}

func (this *subhier) processSubscribe(paths []string, cli *client, qos byte) error {
	if len(paths) == 0 {
		this.subs.add(cli, qos)
		return nil
	}

	path := paths[0]

	this.Lock()
	if _, ok := this.routes[path]; !ok {
		hire := newSubhier()
		this.routes[path] = hire
	}
	this.Unlock()

	return this.routes[path].processSubscribe(paths[1:], cli, qos)
}

func (this *subhier) processUnSubscribe(paths []string, cli *client) error {
	if len(paths) == 0 {
		this.subs.remove(cli)
		return nil
	}

	path := paths[0]
	if _, ok := this.routes[path]; ok {
		return this.routes[path].processUnSubscribe(paths[1:], cli)
	}
	return nil
}

func (this *subhier) search(topic string, qos byte) (result *list.List, err error) {
	tokens, err := topicTokenise(topic)
	if err != nil {
		return
	}
	result = list.New()
	this.match(tokens, result)
	return
}

func (this *subhier) match(paths []string, result *list.List) {
	if len(paths) == 0 {
		for _, sub := range this.subs.subs {
			result.PushBack(sub)
		}
		return
	}

	path := paths[0]

	if wildcards, ok := this.routes["#"]; ok {
		wildcards.match([]string{}, result)
	}

	if wildcards, ok := this.routes["+"]; ok {
		wildcards.match(paths[1:], result)
	}

	if _, ok := this.routes[path]; !ok {
		return
	}

	this.routes[path].match(paths[1:], result)
}

func topicTokenise(topic string) (tokens []string, err error) {
	tokens = strings.Split(topic, "/")

	for i, token := range tokens {
		if token == "$" && i != 0 {
			err = errors.New("$ must at the beginning")
			break
		}

		if token == "#" && i != len(tokens)-1 {
			err = errors.New("# must at the ending")
			break
		}

		if strings.Contains(token, "+") && len(token) != 1 {
			err = errors.New("bad topic")
			break
		}
	}
	return
}

func (this *subhier) clean(c *client) {
	this.subs.remove(c)
	for _, route := range this.routes {
		route.clean(c)
	}
}
