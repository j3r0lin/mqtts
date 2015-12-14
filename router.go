package mqtt

import (
	"container/list"
	"errors"
	"strings"
	"sync"
)

type subscribe struct {
	cid string
	qos byte
}

type subscribes struct {
	sync.RWMutex
	subs map[string]*subscribe
}

func newSubscribes() *subscribes {
	return &subscribes{subs: make(map[string]*subscribe)}
}

func (this *subscribes) add(cid string, qos byte) {
	this.Lock()
	defer this.Unlock()
	if sub, ok := this.subs[cid]; ok {
		sub.qos = qos
	} else {
		this.subs[cid] = &subscribe{cid, qos}
	}
}

func (this *subscribes) remove(cid string) {
	this.Lock()
	defer this.Unlock()
//	for _, sub := range this.subs {
//		if sub.cid == cid {
			delete(this.subs, cid)
//			break
//		}
//	}
}

func (this *subscribes) size() int {
	this.RLock()
	defer this.RUnlock()
	return len(this.subs)
}

//func (this *subscribes) forEach(callback func(string, byte)) {
//	this.RLock()
//	defer this.RUnlock()
//	for _, sub := range this.subs {
//		go callback(sub.cid, sub.qos)
//	}
//}

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
func (this *Server) subscribe(filter string, cid string, qos byte) error {
	if tokens, err := topicTokenise(filter); err != nil {
		return err
	} else {
		this.store.StoreSubscription(filter, cid, qos)
		return this.subhier.subscribe(tokens, cid, qos)
	}
}

// delete a subscribe by topic filter and client id
func (this *Server) unsubscribe(filter string, cid string) error {
	if tokens, err := topicTokenise(filter); err != nil {
		return err
	} else {
		this.store.DeleteSubscription(filter, cid)
		return this.subhier.unsubscribe(tokens, cid)
	}
}

// search the matched subscribe clients by topic name
func (this *Server) subscribers(topic string, qos byte) (result *list.List, err error) {
	tokens, err := topicTokenise(topic)
	if err != nil {
		return
	}
	result = list.New()
	this.subhier.search(tokens, result)
	//todo calculate real qos
	for e := result.Front(); e != nil; e = e.Next() {
		sub := e.Value.(*subscribe)
		e.Value = &subscribe{sub.cid, minQoS(sub.qos, qos)}
	}

	return
}

func (this *Server) cleanSubscriptions(cid string) {
	this.subhier.clean(cid)
	this.store.CleanSubscription(cid)
}

func (this *Server) reloadSubscriptions() {
	this.store.LookupSubscriptions(func(filter, cid string, qos byte){
		tokens, _ := topicTokenise(filter)
		log.Debugf("restore subscription, cid: %v, filter: %v, qos: %v", cid, filter, qos)
		this.subhier.subscribe(tokens, cid, qos)
	})
}


// internal subscribe method, tokens is the result of split topic filter. ie: ["a", "b", "c"] for topic filter "a/b/c"
func (this *subhier) subscribe(tokens []string, cid string, qos byte) error {
	if len(tokens) == 0 {
		this.subs.add(cid, qos)
		return nil
	}

	token := tokens[0]

	this.Lock()
	if _, ok := this.routes[token]; !ok {
		hire := newSubhier()
		this.routes[token] = hire
	}
	this.Unlock()

	return this.routes[token].subscribe(tokens[1:], cid, qos)
}

func (this *subhier) unsubscribe(tokens []string, cid string) error {
	if len(tokens) == 0 {
		this.subs.remove(cid)
		return nil
	}

	token := tokens[0]
	if _, ok := this.routes[token]; ok {
		return this.routes[token].unsubscribe(tokens[1:], cid)
	}
	return nil
}


func (this *subhier) search(tokens []string, result *list.List) {
	if len(tokens) == 0 {
		for _, sub := range this.subs.subs {
			var found bool
			for e := result.Front(); e != nil; e = e.Next() {
				f := e.Value.(*subscribe)
				if f.cid == sub.cid {
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
		wildcards.search([]string{}, result)
	}

	if wildcards, ok := this.routes["+"]; ok {
		wildcards.search(tokens[1:], result)
	}

	if _, ok := this.routes[path]; !ok {
		return
	}

	this.routes[path].search(tokens[1:], result)
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
func (this *subhier) clean(cid string) {
	this.subs.remove(cid)
	for _, route := range this.routes {
		route.clean(cid)
	}
}

func (this *subhier) size() int {
	count := 0
	count += this.subs.size()

	for _, route := range this.routes {
		count += route.size()
	}
	return count
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