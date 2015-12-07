package mqtt

import (
	"errors"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"reflect"
	"strings"
)

func (this *client) process() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("processor: panic, %v", r)
		}
		log.Debugf("processor(%v) stopped, %v, %v", this.id, err, this.Err())
		go this.close()
	}()

	for {
		select {
		case msg := <-this.in:
			log.Debugf("processor(%v): new packet, mid:%v, %v", this.id, msg.Details().MessageID, reflect.TypeOf(msg))
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

		if err = validateTopicAndQos(p.TopicName, p.Qos); err != nil {
			return nil
		}
		log.Debugf("processor(%v) new publish message, mid: %v, topic: %q, qos: %q", this.id, p.MessageID, p.TopicName, p.Qos)

		switch p.Qos {
		case 0:
			// [MQTT-3.3.1-2] the dup must be false if qos is 0
			p.Dup = false
			this.handlePublish(p)
		case 1:
			if p.MessageID == 0 {
				err = ErrInvalidMessageId
				break
			}
			err = this.puback(p.MessageID)
			this.handlePublish(p)
		case 2:
			if p.MessageID == 0 {
				err = ErrInvalidMessageId
				break
			}
			this.handlePublish(p)
			this.server.store.StoreInboundPacket(this.id, p)
			err = this.pubrec(p.MessageID)
		}
	case *packets.PubackPacket:
		this.handlePublished(msg.Details().MessageID)

	case *packets.PubrecPacket:
		err = this.pubrel(msg.Details().MessageID, false)

	case *packets.PubrelPacket:
		mid := msg.Details().MessageID
		if err = this.pubcomp(mid); err != nil {
			this.server.store.DeleteInboundPacket(this.id, mid)
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
		this.will = nil
		return errors.New("Disconnect")
	default:
		err = fmt.Errorf("invalid packets type %s.", reflect.TypeOf(msg))
	}

	if err != nil {
		log.Debugf("processor(%v) Error processing packets: %v", this.id, err)
	}

	return
}

func validateTopicAndQos(topic string, qos byte) error {
	if len(topic) == 0 {
		return ErrInvalidTopicEmptyString
	}

	if topic[0] == '$' {
		return ErrInvalidTopicName
	}

	levels := strings.Split(topic, "/")
	for i, level := range levels {
		if level == "#" && i != len(levels)-1 {
			return ErrInvalidTopicMultilevel
		}
	}

	if qos < 0 || qos > 2 {
		return ErrInvalidQoS
	}
	return nil
}
