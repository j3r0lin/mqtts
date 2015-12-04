package mqtt

import (
	"errors"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"golang.org/x/net/context"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/tomb.v2"
	"io"
	"net"
	"sync"
	"time"
)

type client struct {
	tomb.Tomb
	sync.RWMutex
	ctx     context.Context
	id      string
	address string

	topics    []string
	clean     bool
	will      *packets.PublishPacket
	keepalive time.Duration

	server *Server

	connected bool
	stopOnce  sync.Once

	conn net.Conn
	opts *Options

	flight map[uint16]*packets.PublishPacket

	in  chan packets.ControlPacket
	out chan packets.ControlPacket
}

func (this *client) start() (err error) {
	this.in = make(chan packets.ControlPacket)
	this.out = make(chan packets.ControlPacket)

	this.address = this.conn.RemoteAddr().String()
	this.keepalive = this.opts.ConnectTimeout
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
func (this *client) stop() error {
	this.Lock()
	defer this.Unlock()

	log.Debugf("client(%v) stop called", this.id)
//	if this.connected && this.Alive() {
//		this.Kill(errors.New("shutdown"))
//		return this.Wait()
//	}
//	this.close()
	this.Kill(errors.New("shutdown"))
	this.close()
	return nil
}

// release all resources
func (this *client) close() {
	this.stopOnce.Do(func(){
		this.conn.Close()
		this.Wait()
		err := this.Err()
		log.Debugf("conn(%v) closing %v", this.id, err)
		close(this.in)
		close(this.out)

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
		cp.ClientIdentifier = bson.NewObjectId().Hex()
	}

	this.id = cp.ClientIdentifier
	if cp.KeepaliveTimer != 0 {
		this.keepalive = time.Duration(cp.KeepaliveTimer) * time.Second
	} else {
		this.keepalive = this.opts.KeepAlive
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

func (this *client) handleSubscribe(msgid uint16, topics []string, qoss []byte) error {
	for index, topic := range topics {
		log.Debugf("client(%v) subscribe to %q qos %q", this.id, topic, qoss[index])
		if err := this.server.subhier.subscribe(topic, this, qoss[index]); err != nil {
			log.Warnf("client(%v) sub to %q failed, %v, disconnecting", this.id, topic, err)
			return err
		}
		//		this.topics = append(this.topics, topic)
		matchRetain(topic, func(message *packets.PublishPacket) {
			this.publish(message.TopicName, message.Payload, message.Qos, message.MessageID, message.Retain)
		})
	}
	this.suback(msgid, qoss)
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

	this.server.storePacket(message)
	// forward message to all subscribers
	this.server.forwardMessage(message)

	return nil
}

func (this *client) handlePublished(messageId uint16) error {
	this.server.deleteOfflinePacket(this.id, messageId)
	return nil
}
