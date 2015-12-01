package mqtt

import (
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"strings"
	"container/list"
)

var defaultRetains *retains

func init() {
	defaultRetains = newRetains()
}

type retains struct {
	children map[string]*retains
	message  *packets.PublishPacket
}

func newRetains() *retains {
	return &retains{
		children: make(map[string]*retains),
	}
}

func retain(message *packets.PublishPacket) error {
	tokens, err := topicTokenise(message.TopicName)
	if err != nil {
		return err
	}
	defaultRetains.retain(tokens, message)
	return nil
}

func matchRetain(topic string, callback func(*packets.PublishPacket)) error {
	tokens, err := topicTokenise(topic)
	if err != nil {
		return err
	}
	l := list.New()
	defaultRetains.match(tokens, l)
	for e := l.Front(); e != nil; e = e.Next() {
		if message, ok := e.Value.(*packets.PublishPacket); ok {
			callback(message)
		}
	}
	return nil
}

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

func (this *retains) remains(result *list.List) {
	if this.message != nil {
		result.PushBack(this.message)
	}

	for _, child := range this.children {
		child.remains(result)
	}
}

func (this *retains) print(level int) {
	prefix := (strings.Repeat(" ", level))
	for path, val := range this.children {
		log.Print(prefix, "|-", path)
		val.print(level + 1)
	}
}
