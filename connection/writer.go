package conn

import (
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"io"
	"sync/atomic"
)

func (this *Connection) writer() (err error) {
	defer func() {
		if r := recover(); r != nil {
			this.workerExit(fmt.Errorf("mqtt:reader:panic with %v", r))
		}
		log.Debugf("writer: closed, %v, (%v)", err, this.id)
		this.workers.Done()
	}()

	var cp packets.ControlPacket

	for {
		select {
		case cp = <-this.out:
			log.Debug("writer: sending message, ", cp.Details().MessageID)
			if err = this.writePacket(cp); err != nil {
				if err != io.EOF {
					log.Warnln("mqtt:writer:error reading from connection", err, this.id)
				}
				this.workerExit(err)
				return
			}
		case <-this.stop:
			return
		}
	}

}

func (this *Connection) writePacket(p packets.ControlPacket) error {
	if err := p.Write(this.conn); err != nil {
		return err
	}
	return nil
}

func (this *Connection) Write(p packets.ControlPacket) error {
	this.out <- p
	log.Debug("writer: message sended to queue")
	return nil
}

func (this *Connection) Connack(returnCode byte, sessionPresent bool) error {
	p := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	p.ReturnCode = returnCode
	if sessionPresent && returnCode == packets.Accepted {
		p.TopicNameCompression = 1
	}
	return this.writePacket(p)
}

func (this *Connection) Pingresp() error {
	p := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	return this.Write(p)
}
var globalMessageId uint64 = 1

func (this *Connection) Publish(topic string, payload []byte, qos byte, messageId uint16, retain bool) error {
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = topic
	p.Payload = payload
	p.Qos = qos
	if messageId == 0 && qos > 0 {
		messageId = uint16(atomic.AddUint64(&globalMessageId, 1) & 0xffff)
	}
	p.MessageID = messageId
	p.Retain = retain
	return this.Write(p)
}

func (this *Connection) Puback(messageId uint16) error {
	cp := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	cp.MessageID = messageId
	return this.Write(cp)
}

func (this *Connection) Pubrec(messageId uint16) error {
	cp := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	cp.MessageID = messageId
	return this.Write(cp)
}

func (this *Connection) Pubcomp(messageId uint16) error {
	cp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	cp.MessageID = messageId
	return this.Write(cp)
}

func (this *Connection) Pubrel(messageId uint16, dup bool) error {
	cp := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	cp.MessageID = messageId
	cp.Dup = dup
	return this.Write(cp)
}

func (this *Connection) Unsuback(messageId uint16) error {
	cp := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	cp.MessageID = messageId
	return this.Write(cp)
}

func (this *Connection) Suback(messageId uint16, qoss []byte) error {
	cp := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	cp.MessageID = messageId
	cp.GrantedQoss = qoss
	return this.Write(cp)
}
