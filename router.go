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

// add subscribe to the tree, a subscribe include a topic filter, qos and client id
func (this *subhier) subscribe(filter string, cli *client, qos byte) error {
	if tokens, err := topicTokenise(filter); err != nil {
		return err
	} else {
		return this.processSubscribe(tokens, cli, qos)
	}
}

// delete a subscribe by topic filter and client id
func (this *subhier) unsubscribe(filter string, cli *client) error {
	if tokens, err := topicTokenise(filter); err != nil {
		return err
	} else {
		return this.processUnSubscribe(tokens, cli)
	}
}

// internal subscribe method, tokens is the result of split topic filter. ie: ["a", "b", "c"] for topic filter "a/b/c"
func (this *subhier) processSubscribe(tokens []string, cli *client, qos byte) error {
	if len(tokens) == 0 {
		this.subs.add(cli, qos)
		return nil
	}

	token := tokens[0]

	this.Lock()
	if _, ok := this.routes[token]; !ok {
		hire := newSubhier()
		this.routes[token] = hire
	}
	this.Unlock()

	return this.routes[token].processSubscribe(tokens[1:], cli, qos)
}

func (this *subhier) processUnSubscribe(tokens []string, cli *client) error {
	if len(tokens) == 0 {
		this.subs.remove(cli)
		return nil
	}

	token := tokens[0]
	if _, ok := this.routes[token]; ok {
		return this.routes[token].processUnSubscribe(tokens[1:], cli)
	}
	return nil
}

// search the matched subscribe clients by topic name
func (this *subhier) search(topic string, qos byte) (result *list.List, err error) {
	tokens, err := topicTokenise(topic)
	if err != nil {
		return
	}
	result = list.New()
	this.match(tokens, result)
	//todo calculate real qos
	for e := result.Front(); e != nil; e = e.Next() {
		sub := e.Value.(*subscribe)
		e.Value = &subscribe{sub.client, minQoS(sub.qos, qos)}
	}

	return
}

func (this *subhier) match(tokens []string, result *list.List) {
	if len(tokens) == 0 {
		for _, sub := range this.subs.subs {
			var found bool
			for e := result.Front(); e != nil; e = e.Next() {
				f := e.Value.(*subscribe)
				if f.client.id == sub.client.id {
					found = true
					e.Value.(*subscribe).qos = maxQoS(sub.qos, f.qos)
				}
			}

			if !found {
				result.PushBack(sub)
			}
		}
		return
	}

	path := tokens[0]

	if wildcards, ok := this.routes["#"]; ok {
		wildcards.match([]string{}, result)
	}

	if wildcards, ok := this.routes["+"]; ok {
		wildcards.match(tokens[1:], result)
	}

	if _, ok := this.routes[path]; !ok {
		return
	}

	this.routes[path].match(tokens[1:], result)
}

// split the topic name or topic filter to tokens, also validate topic rules.
func topicTokenise(topic string) (tokens []string, err error) {
	tokens = strings.Split(topic, "/")

	for i, token := range tokens {
		if token == "$" && i != 0 {
			err = errors.New("invalid topic filter, $ must at the beginning")
			break
		}

		if token == "#" && i != len(tokens) - 1 {
			err = errors.New("invalid topic filter, # must at the ending")
			break
		}

		if strings.Contains(token, "+") && len(token) != 1 {
			err = errors.New("invalid topic filter")
			break
		}
	}
	return
}

// remove all subscriptions of a client
func (this *subhier) clean(c *client) {
	this.subs.remove(c)
	for _, route := range this.routes {
		route.clean(c)
	}
}


func minQoS(qos1, qos2 byte) byte{
	if qos1 < qos2 {
		return qos1
	}
	return qos2
}

func maxQoS(qos1, qos2 byte) byte{
	if qos1 > qos2 {
		return qos1
	}
	return qos2
}