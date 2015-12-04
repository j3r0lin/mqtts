package mqtt

import (
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"io"
	"reflect"
)

func (this *client) writer() (err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			err = fmt.Errorf("writer panic with %v", r)
		}
		log.Debugf("writer(%v) stopped, %v, %v", this.id, err, this.Err())
	}()

	var cp packets.ControlPacket

	for {
		select {
		case cp = <-this.out:
			log.Debugf("writer(%v) sending message %v, id: %q", this.id, reflect.TypeOf(cp), cp.Details().MessageID)
			if err = this.writePacket(cp); err != nil {
				if err != io.EOF {
					log.Warnf("writer(%v) writting message to connection err, %v", this.id, err)
				}
				return
			}
		case <-this.Dying():
			return
		}
	}

}

func (this *client) writePacket(p packets.ControlPacket) error {
	if err := p.Write(this.conn); err != nil {
		return err
	}
	return nil
}

func (this *client) write(p packets.ControlPacket) (err error) {
	defer func(){
		if r := recover(); r != nil {
			err = ErrDisconnect
		}
	}()

//	select {
//	case this.out <- p:
//		log.Debugf("writer(%v): message %v sended to queue", this.id, reflect.TypeOf(p))
//	default:
//	}
	// we need to wait for pre message flushed if channel is full
	this.out <- p
	log.Debugf("writer(%v): message %v, id: %v sended to queue", this.id, reflect.TypeOf(p), p.Details().MessageID)
	return
}

func (this *client) connack(returnCode byte, sessionPresent bool) error {
	p := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	p.ReturnCode = returnCode
	if sessionPresent && returnCode == packets.Accepted {
		p.TopicNameCompression = 0x01
	}
	return this.writePacket(p)
}

func (this *client) pingresp() error {
	p := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	return this.write(p)
}

func (this *client) publish(topic string, payload []byte, qos byte, retain bool, dup bool) error {
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = topic
	p.Payload = payload
	p.Qos = qos
	p.Retain = retain
	p.Dup = dup

	if qos > 0 {
		p.MessageID = this.messageIds.use(p.UUID())
	}
	return this.write(p)
}

func (this *client) puback(messageId uint16) error {
	cp := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	cp.MessageID = messageId
	return this.write(cp)
}

func (this *client) pubrec(messageId uint16) error {
	cp := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	cp.MessageID = messageId
	return this.write(cp)
}

func (this *client) pubcomp(messageId uint16) error {
	cp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	cp.MessageID = messageId
	return this.write(cp)
}

func (this *client) pubrel(messageId uint16, dup bool) error {
	cp := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	cp.MessageID = messageId
	cp.Dup = dup
	return this.write(cp)
}

func (this *client) unsuback(messageId uint16) error {
	cp := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	cp.MessageID = messageId
	return this.write(cp)
}

func (this *client) suback(messageId uint16, qoss []byte) error {
	cp := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	cp.MessageID = messageId
	cp.GrantedQoss = qoss
	return this.write(cp)
}
