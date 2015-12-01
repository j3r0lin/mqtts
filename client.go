package mqtt

import (
	"bitbucket.org/j3r0lin/mqtt/connection"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"net"
	"sync"
	"errors"
)

type client struct {
	id string

	opts    *ServerOpts
	conn    *conn.Connection
	started sync.WaitGroup
	stopped sync.WaitGroup

	topics []string
	clean  bool
	will   *packets.PublishPacket

	server *Server

	mu     sync.Mutex
	closed bool
	stop   chan struct{}
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
	conn.PubackHandler = this.handlePuback
	conn.ConnectionLostHandler = this.handleConnectionLost

	if err := conn.Start(); err != nil {
		log.Warnln("client start connection failed", err)
		return err
	}

	return nil
}

func (this *client) handleConnectionLost(err error) {
	if this.will != nil {
		this.handlePublish(this.will)
	}

	this.server.mu.Lock()
	delete(this.server.clients, this.id)
	this.server.mu.Unlock()
	if this.clean {
		this.server.cleanSeassion(this)
	}

	this.closed = true

	log.Infoln("client disconnect", err)
}

func (this *client) handleConnectPacket(p *packets.ConnectPacket) error {
	//fixme validate username & password

	//fixme init session
	id := p.ClientIdentifier
	this.id = id

	this.clean = p.CleanSession

	if p.WillFlag && len(p.WillTopic) != 0 {
		will := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		will.TopicName = p.WillTopic
		will.Payload = p.WillMessage
		will.Qos = p.WillQos
		will.Retain = p.WillRetain

		this.will = will
	}

//	this.server.mu.Lock()
//	defer this.server.mu.Unlock()
	if _, ok := this.server.clients[id]; ok {
//		this.server.mu.Unlock()
//		pre.conn.Stop()
		return errors.New("already connect")
	} else {
		this.server.clients[id] = this
//		this.server.mu.Unlock()
	}

	log.Infof("client connect %q, %q %q, clean %v", p.Username, string(p.Password), p.ClientIdentifier, this.clean)
	if this.clean {
		this.server.cleanSeassion(this)
		this.conn.Connack(packets.Accepted, false)
	} else {
		this.conn.Connack(packets.Accepted, true)
		this.server.forwardOfflineMessage(this)
	}

	return nil
}

func (this *client) handleSubscribe(msgid uint16, topics []string, qoss []byte) error {
	for index, topic := range topics {
		log.Debugln("client", this.id, "subscribe to", topic, "qos", qoss[index])
		if err := this.server.subhier.subscribe(topic, this, qoss[index]); err != nil {
			log.Warnln("sub failed, disconnecting", err)
			return err
		}
		this.topics = append(this.topics, topic)
		matchRetain(topic, func(message *packets.PublishPacket) {
			this.conn.Publish(message.TopicName, message.Payload, message.Qos, message.MessageID, message.Retain)
		})
	}
	this.conn.Suback(msgid, qoss)
	return nil
}

func (this *client) handlePublish(message *packets.PublishPacket) error {
	log.Debugf("publish messge received, topic: %q, id: %q, cid(%v)", message.TopicName, message.MessageID, this.id)
	this.server.storePacket(message)

	// forward message to all subscribers
	this.server.forwardMessage(message)

	return nil
}

func (this *client) handlePuback(messageId uint16) error {
	this.server.deleteOfflinePacket(this.id, messageId)
	return nil
}
