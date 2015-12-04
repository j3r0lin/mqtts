package mqtt

import (
	"errors"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"reflect"
)

func (this *client) process() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("processor: panic")
		}
		log.Debugf("processor(%v) stopped, %v, %v", this.id, err, this.Err())
		go this.close()
	}()

	for {
		select {
		case msg := <-this.in:
			log.Debugf("processor(%v): new packet, id:%v, %v", this.id, msg.Details().MessageID, reflect.TypeOf(msg))
			if err = this.processPacket(msg); err != nil {
				return
			}
		case <-this.Dying():
			return
		}
	}
}

func (this *client) processPacket(msg packets.ControlPacket) (err error) {
	switch msg.(type) {
	case *packets.PublishPacket:
		p := msg.(*packets.PublishPacket)
		log.Debugf("processor(%v) new publish message, msgid: %q, topic: %q, qos: %q", this.id, p.MessageID, p.TopicName, p.Qos)

		switch p.Qos {
		case 0:
			this.handlePublish(p)
		case 1:
			if p.MessageID == 0 {
				err = ErrMessageIdInvalid
				break
			}
			err = this.puback(p.MessageID)
			this.handlePublish(p)
		case 2:
			if p.MessageID == 0 {
				err = ErrMessageIdInvalid
				break
			}
			err = this.pubrec(p.MessageID)
			this.handlePublish(p)
		}
	case *packets.PubackPacket:
		this.handlePublished(msg.Details().MessageID)

	case *packets.PubrecPacket:
		err = this.pubrel(msg.Details().MessageID, false)

	case *packets.PubrelPacket:
		if err = this.pubcomp(msg.Details().MessageID); err == nil {
			//			this.handlePublish(cp)
		}
	case *packets.PubcompPacket:
		this.handlePublished(msg.Details().MessageID)
	case *packets.SubscribePacket:
		p := msg.(*packets.SubscribePacket)
		this.handleSubscribe(p.MessageID, p.Topics, p.Qoss)

		//	case *packets.SubackPacket:
	case *packets.UnsubscribePacket:
		p := msg.(*packets.UnsubscribePacket)
		this.handleUnsubscribe(p.Topics)
		err = this.unsuback(p.MessageID)
		//	case *packets.UnsubackPacket:

	case *packets.PingreqPacket:
		err = this.pingresp()
		//	case *packets.PingrespPacket:

	case *packets.DisconnectPacket:
		return errors.New("Disconnect")
	default:
		err = fmt.Errorf("invalid packets type %s.", reflect.TypeOf(msg))
	}

	if err != nil {
		log.Debugf("processor(%v) Error processing packets: %v", this.id, err)
	}

	return
}
