package mqtt

import (
	"bitbucket.org/j3r0lin/mqtt/connection"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"github.com/dropbox/godropbox/errors"
	"net"
	"sync"
)

type client struct {
	id      string

	opts    *ServerOpts
	conn    *conn.Connection
	started sync.WaitGroup
	stopped sync.WaitGroup

	topics  []string

	server  *Server

	mu      sync.Mutex
	closed  bool
	stop    chan struct{}
}

func (this *client) start(c net.Conn) error {
	var opts = this.opts
	var conn = conn.NewConnection(c, &conn.Options{
		KeepAlive:      opts.KeepAlive,
		ConnectTimeout: opts.ConnectTimeout,
		AckTimeout:     opts.AckTimeout,
		TimeoutRetries: opts.TimeoutRetries,
	})
	this.conn = conn

	conn.ConnectHandler = this.handleConnectPacket
	conn.SubscribeHandler = this.handleSubscribe
	conn.PublishHandler = this.handlePublish
	conn.ConnectionLostHandler = this.handleConnectionLost

	if err := conn.Start(); err != nil {
		log.Warnln("client start connection failed", err)
		conn.Stop()
		return err
	}

	return nil
}

func (this *client) handleConnectionLost(err error) {
	this.server.mu.Lock()
	delete(this.server.clients, this.id)
	this.server.mu.Unlock()
	for _, topic := range this.topics {
		this.server.subhier.unsubscribe(topic, this)
	}

	log.Infoln("client disconnect", err)
}

func (this *client) handleConnectPacket(p *packets.ConnectPacket) error {
	//fixme validate username & password

	//fixme init session
	id := p.ClientIdentifier
	this.id = id

	this.server.mu.Lock()
	if _, ok := this.server.clients[id]; ok {
		this.server.mu.Unlock()
		return errors.New("already connect")
	} else {
		this.server.clients[id] = this
		this.server.mu.Unlock()
	}

	log.Infof("client connect %q, %q %s", p.Username, string(p.Password), p.ClientIdentifier)
	return this.conn.Connack(packets.Accepted, false)
}

func (this *client) handleSubscribe(msgid uint16, topics []string, qoss []byte) error {
	for index, topic := range topics {
		log.Debugln("client", this.id, "subscribe to", topic, "qos", qoss[index])
		if err := this.server.subhier.subscribe(topic, this, qoss[index]); err != nil {
			log.Warnln("sub failed, disconnecting", err)
			return err
		}
		this.topics = append(this.topics, topic)
	}
	this.conn.Suback(msgid, qoss)
	return nil
}

func (this *client ) handlePublish(msgId uint16, topic string, payload []byte, qos byte, retain bool, dup bool) error {
	log.Debugf("publish messge received, topic: %q, id: %q, cid(%v)", topic, msgId, this.id)
	this.server.subhier.search(topic, qos, func(subs *subscribes) {
		subs.forEach(func(cli *client, qos byte) {
			log.Debugf("forward message to %q, topic: %q, qos: %q, msgid: %q ", cli.id, topic, qos, msgId)
			cli.conn.Publish(topic, payload, qos, msgId, retain)
		})
	})
	return nil
}
