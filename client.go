package mqtt

import (
	"errors"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"golang.org/x/net/context"
	"gopkg.in/tomb.v2"
	"io"
	"net"
	"sync"
	"time"
	"github.com/pborman/uuid"
)

type client struct {
	tomb.Tomb
	sync.RWMutex
	ctx       context.Context
	id        string
	address   string

	topics    []string
	clean     bool
	will      *packets.PublishPacket
	keepAlive time.Duration

	server    *Server

	connected bool
	stopOnce  sync.Once

	conn      net.Conn
	opts      *Options

	in        chan packets.ControlPacket
	out       chan packets.ControlPacket
}

func (this *client) start() (err error) {
	this.in = make(chan packets.ControlPacket)
	this.out = make(chan packets.ControlPacket)

	this.address = this.conn.RemoteAddr().String()
	this.keepAlive = this.opts.ConnectTimeout
	if err = this.waitConnect(); err != nil {
		log.Debugf("client(%v) connect processing failed, %v", this.id, err)
		this.conn.Close()
		return
	}
	this.Go(this.reader)
	this.Go(this.writer)
	this.Go(this.process)

	this.connected = true
	this.server.clients.add(this)

	return
}

// shutdown the client when it's running
func (this *client) stop(err error) error {
	this.Lock()
	defer this.Unlock()

	log.Debugf("client(%v) stop called", this.id)
	//	if this.connected && this.Alive() {
	//		this.Kill(errors.New("shutdown"))
	//		return this.Wait()
	//	}
	//	this.close()
	if err == nil {
		err = errors.New("shutdown")
	}
	this.Kill(err)
	this.close()
	return nil
}

// release all resources
func (this *client) close() {
	this.stopOnce.Do(func() {
		this.conn.Close()
		close(this.in)
		close(this.out)

		this.Wait()
		err := this.Err()
		log.Debugf("conn(%v) closing %v", this.id, err)

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			log.Infof("conn(%v) client closed the connection", this.id)
		} else {
			log.Infof("conn(%v) client connection closed, %v", this.id, err)
		}

		this.handleDisconnect(err)
	})
}

func (this *client) waitConnect() (err error) {
	var cp *packets.ConnectPacket

	if cp, err = this.ReadConnectPacket(); err != nil {
		return
	}

	if code := cp.Validate(); code != packets.Accepted {
		this.connack(code, false)
		return fmt.Errorf("client(%v) bad connect packet %x, %q", cp.ClientIdentifier, code)
	}

	if len(cp.ClientIdentifier) == 0 {
		if !cp.CleanSession {
			this.connack(packets.ErrRefusedIDRejected, false)
			return ErrRefusedClientId
		}
		cp.ClientIdentifier = uuid.New()
	}


	this.id = cp.ClientIdentifier
	if cp.KeepaliveTimer != 0 {
		this.keepAlive = time.Duration(cp.KeepaliveTimer) * time.Second
	} else {
		this.keepAlive = this.opts.KeepAlive
	}
	this.clean = cp.CleanSession

	if cp.WillFlag && len(cp.WillTopic) != 0 {
		will := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		will.TopicName = cp.WillTopic
		will.Payload = cp.WillMessage
		will.Qos = cp.WillQos
		will.Retain = cp.WillRetain

		this.will = will
	}

	log.Infof("client(%v) connect as %q, %q, clean %v, from %v", this.id, cp.Username, string(cp.Password), this.clean, this.address)
	if this.clean {
		this.server.cleanSession(this)
		this.connack(packets.Accepted, false)
	} else {
		this.connack(packets.Accepted, true)
		go this.server.forwardOfflineMessage(this)
	}

	return nil
}

func (this *client) handleDisconnect(err error) {
	if this.will != nil {
		this.handlePublish(this.will)
	}

	this.server.clients.delete(this)

	if this.clean {
		this.server.cleanSession(this)
	}

	this.connected = false

	log.Infof("client(%v) disconnect, %v", this.id, err)
}

func (this *client) handleSubscribe(msgid uint16, topic string, qos byte) error {
	log.Debugf("client(%v) subscribe to %q qos %q", this.id, topic, qos)
	if err := this.server.subhier.subscribe(topic, this, qos); err != nil {
		log.Warnf("client(%v) sub to %q failed, %v, disconnecting", this.id, topic, err)
		return err
	}
	matchRetain(topic, func(m *packets.PublishPacket) {
		// we should choose the min one as qos to send this message.
		qos = minQoS(qos, m.Qos)

		log.Debugf("client(%v) matched retain message, topic: %v, qos: %v, mid: %v", this.id, m.TopicName, qos, m.MessageID)
		// for new subscribe client, the retained should be true.
		this.publish(m.TopicName, m.Payload, qos, true, m.Dup)
	})
	return nil
}

func (this *client) handleUnsubscribe(topics []string) error {
	for _, topic := range topics {
		log.Debugf("client(%v) unsub to %q qos %q", this.id, topic)
		if err := this.server.subhier.unsubscribe(topic, this); err != nil {
			log.Warnf("client(%v) unsub to %q failed, %v, disconnecting", this.id, topic, err)
			return err
		}

	}
	return nil
}

func (this *client) handlePublish(message *packets.PublishPacket) error {
	log.Debugf("client(%v) publish messge received, topic: %q, id: %q", this.id, message.TopicName, message.MessageID)
	// forward message to all subscribers
	if message.Retain {
		retain(message)
	}

	this.server.forwardMessage(message)
	return nil
}

func (this *client) handlePublished(mid uint16) error {
	this.server.mids.free(this.id, mid)
	this.server.store.DeleteOutboundPacket(this.id, mid)
	return nil
}
