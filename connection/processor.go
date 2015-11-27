package conn

import (
	"errors"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"reflect"
)

func (this *Connection) process() (err error) {
	defer func() {
		log.Debugln("processor: closed", err)
		this.workers.Done()
		if r := recover(); r != nil {
			err = errors.New("processor: panic")
		}
		this.close(err)
	}()

	for {
		select {
		case msg := <-this.in:
			log.Debugln("processor: processing new packet", msg.Details().MessageID, reflect.ValueOf(msg).Type())
			if err = this.processPacket(msg); err != nil {
				//					if err == errDisconnect {
				//						log.Errorf("(%s) Error processing %s: %v", this.cid(), msg.Name(), err)
				//					} else {
				//						return
				//					}
				return
			}
		case <-this.stop:
			err = errors.New("stop message")
			return
		case err = <-this.errors:
			return
		}
	}
}

func (this *Connection) processPacket(msg packets.ControlPacket) (err error) {
	switch msg.(type) {
	case *packets.PublishPacket:
		log.Debugln("processor: received publish message")
		p := msg.(*packets.PublishPacket)
		switch p.Qos {
		case 0:
			break
		case 1:
			err = this.Puback(p.MessageID)
		case 2:
			err = this.Pubrec(p.MessageID)
		}
	case *packets.PubackPacket:

	case *packets.PubrecPacket:
		err = this.Pubrel(msg.Details().MessageID, false)
	case *packets.PubrelPacket:
		err = this.Pubcomp(msg.Details().MessageID)
	case *packets.PubcompPacket:

	case *packets.SubscribePacket:

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
