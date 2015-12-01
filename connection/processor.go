package conn

import (
	"errors"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"reflect"
)

func (this *Connection) process() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("processor: panic")
		}
		log.Debugf("processor of %q stopped, %v, %v", this.id, err, this.Err())
		this.conn.Close()
		go func() {
			this.Wait()
			this.close()
		}()
	}()

	for {
		select {
		case <- this.Dying():
			return
		case msg := <-this.in:
			log.Debugf("processor(%v): processing new packet, id:%v, %v", this.id, msg.Details().MessageID, reflect.ValueOf(msg).Type())
			if err = this.processPacket(msg); err != nil {
				return
			}
		}
	}
}

func (this *Connection) processPacket(msg packets.ControlPacket) (err error) {
	switch msg.(type) {
	case *packets.PublishPacket:
		p := msg.(*packets.PublishPacket)
		log.Debugf("processor: received publish message, msgid: %q, topic: %q, qos: %q, cid(%s)", p.MessageID, p.TopicName, p.Qos, this.id)

		switch p.Qos {
		case 0:
			if this.PublishHandler != nil {
				this.PublishHandler(p)
			}
		case 1:
			if p.MessageID == 0 {
				err = ErrMessageIdInvalid
				break
			}
			err = this.Puback(p.MessageID)

			if this.PublishHandler != nil {
				this.PublishHandler(p)
			}
		case 2:
			if p.MessageID == 0 {
				err = ErrMessageIdInvalid
				break
			}
			err = this.Pubrec(p.MessageID)
			if this.PublishHandler != nil {
				this.PublishHandler(p)
			}
		}
	case *packets.PubackPacket:
		if this.PubackHandler != nil {
			p := msg.(*packets.PubackPacket)
			err = this.PubackHandler(p.MessageID)
		}
	case *packets.PubrecPacket:
		err = this.Pubrel(msg.Details().MessageID, false)
	case *packets.PubrelPacket:
		err = this.Pubcomp(msg.Details().MessageID)
	case *packets.PubcompPacket:

	case *packets.SubscribePacket:
		p := msg.(*packets.SubscribePacket)
		if this.SubscribeHandler != nil {
			err = this.SubscribeHandler(p.MessageID, p.Topics, p.Qoss)
		}
	case *packets.SubackPacket:

	case *packets.UnsubscribePacket:

	case *packets.UnsubackPacket:

	case *packets.PingreqPacket:
		err = this.Pingresp()
	case *packets.PingrespPacket:

	case *packets.DisconnectPacket:
		return errors.New("Disconnect")
	default:
		return fmt.Errorf("(%s) invalid packets type %s.", this.id)
	}

	if err != nil {
		log.Debugf("(%s) Error processing acked packets: %v", this.id, err)
	}

	return
}
