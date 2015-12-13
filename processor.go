package mqtt

import (
	"errors"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"reflect"
	"strings"
	"unicode/utf8"
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
			// [MQTT-3.3.1-4] A PUBLISH Packet MUST NOT have both QoS bits set to 1. If a Server or Client receives a PUBLISH
			// Packet which has both QoS bits set to 1 it MUST close the Network Connection
			if err == ErrInvalidQoS {
				return err
			}
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

			// ensure this packet is or not duplicate resend.
			if this.server.store.FindInboundPacket(this.id, p.MessageID) == nil {
				this.handlePublish(p)
				this.server.store.StoreInboundPacket(this.id, p)
			}
			err = this.pubrec(p.MessageID)
		}
	case *packets.PubackPacket:
		this.handlePublished(msg.Details().MessageID)

	case *packets.PubrecPacket:
		err = this.pubrel(msg.Details().MessageID, false)

	case *packets.PubrelPacket:
		mid := msg.Details().MessageID
		err = this.pubcomp(mid)
		if err == nil {
			this.server.store.DeleteInboundPacket(this.id, mid)
		}

	case *packets.PubcompPacket:
		this.handlePublished(msg.Details().MessageID)
	case *packets.SubscribePacket:
		p := msg.(*packets.SubscribePacket)

		if err = validateSubscriptions(p.Topics, p.Qoss); err != nil {
			return err
		}

		qoss := make([]byte, len(p.Topics))
		for index, topic := range p.Topics {
			qos := p.Qoss[index]
			if err = validateTopicAndQos(topic, qos); err != nil {
				qoss[index] = 0x80
				continue
			}
			this.handleSubscribe(p.MessageID, topic, qos)
			qoss[index] = qos
		}

		this.suback(p.MessageID, qoss)

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

func validateSubscriptions(topics []string, qoss []byte) error {
	if len(qoss) != len(topics) {
		return ErrInvalidSubscriber
	}

	if len(topics) == 0 {
		return ErrInvalidSubscriber
	}
	return nil
}

func validateTopicAndQos(topic string, qos byte) error {
	if err := validateTopicFilter(topic); err != nil {
		return nil
	}
	return validateQoS(qos)
}

func validateTopic(topic string) error {
	if len(topic) == 0 {
		return ErrInvalidTopicEmptyString
	}

	if utf8.ValidString(topic) == false {
		return ErrInvalidTopicName
	}
	return nil
}

func validateTopicFilter(filter string) error {
	if err := validateTopic(filter); err != nil {
		return err
	}

	if filter[0] == '$' {
		return ErrInvalidTopicName
	}

	levels := strings.Split(filter, "/")
	for i, level := range levels {
		if level == "#" && i != len(levels)-1 {
			return ErrInvalidTopicMultilevel
		}

		if strings.Contains(level, "+") && len(level) != 1 {
			return ErrInvalidTopicMultilevel
		}
	}
	return nil
}

func validateQoS(qos byte) error {
	if qos < 0 || qos > 2 {
		return ErrInvalidQoS
	}
	return nil
}