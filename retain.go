package mqtt

import (
	"container/list"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"strings"
)

// store retain packet into memory cache and backend store.
func (this *Server) retainPacket(p *packets.PublishPacket) error {
	tokens, err := topicTokenise(p.TopicName)
	if err != nil {
		return err
	}
	this.retains.retain(tokens, p)
	this.store.StoreRetained(p)
	return nil
}

// reload all stored retain messages to memory cache
func (this *Server) reloadRetains() {
	this.store.LookupRetained(func(p *packets.PublishPacket) {
		tokens, _ := topicTokenise(p.TopicName)
		this.retains.retain(tokens, p)
	})
}

// match retain messages by topic filter.
func (this *Server) matchRetain(filter string, callback func(*packets.PublishPacket)) error {
	tokens, err := topicTokenise(filter)
	if err != nil {
		return err
	}
	l := list.New()
	this.retains.match(tokens, l)
	for e := l.Front(); e != nil; e = e.Next() {
		if message, ok := e.Value.(*packets.PublishPacket); ok {
			callback(message)
		}
	}
	return nil
}

// a memory tree to store retain messages, easy to match topic name by filter.
type retains struct {
	children map[string]*retains
	message  *packets.PublishPacket
}

func newRetains() *retains {
	return &retains{
		children: make(map[string]*retains),
	}
}

// store a retain message, remove retain message if message body is empty
// the tokens is splits of topic name divide by '/'
func (this *retains) retain(tokens []string, message *packets.PublishPacket) bool {
	if len(tokens) == 0 {
		this.message = message
		if message == nil || len(message.Payload) == 0 {
			return true
		}
		return false
	}
	path := tokens[0]

	if _, ok := this.children[path]; !ok {
		this.children[path] = newRetains()
	}

	if this.children[path].retain(tokens[1:], message) {
		delete(this.children, path)
	}

	if len(this.children) == 0 && this.message == nil {
		return true
	}

	return false
}

// match all topic by the given topic filter and return the matched retain messages.
func (this *retains) match(tokens []string, result *list.List) {
	if len(tokens) == 0 {
		if this.message != nil {
			result.PushBack(this.message)
		}
		return
	}

	token := tokens[0]

	switch token {
	case "#":
		this.remains(result)
	case "+":
		for _, child := range this.children {
			child.match(tokens[1:], result)
		}
	default:
		if child, ok := this.children[token]; ok {
			child.match(tokens[1:], result)
		}
	}
}

// add all remain messages to result list. only if matched a '#' wildcard
func (this *retains) remains(result *list.List) {
	if this.message != nil {
		result.PushBack(this.message)
	}

	for _, child := range this.children {
		child.remains(result)
	}
}

// debug usage. print the tree.
func (this *retains) print(level int) {
	prefix := (strings.Repeat(" ", level))
	for path, val := range this.children {
		log.Print(prefix, "|-", path)
		val.print(level + 1)
	}
}

func (this *retains) size() int {
	count := 0
	if this.message != nil {
		count++
	}
	for _, child := range this.children {
		count += child.size()
	}
	return count
}
