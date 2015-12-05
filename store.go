package mqtt

import (
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"sync"
	"fmt"
)

type store struct {
	sync.RWMutex
	messages map[string]map[string]packets.ControlPacket
}

func newStore() *store {
	store := &store{
		messages: make(map[string]map[string]packets.ControlPacket),
	}
	return store
}

func (this *store) StoreSubscriptions(client *client) {
	if client.clean {
		return
	}
}

func (this *store) LookupSubscriptions(client *client) {

}

func (this *store) StoreRetained(message *packets.PublishPacket) {

}

func (this *store) LookupRetained(topic string, callback func(*packets.PublishPacket)) {

}

func (this *store ) FindInboundPacket(cid string, mid uint16) packets.ControlPacket {
	this.Lock()
	defer this.Unlock()
	if _, ok := this.messages[cid]; ok {
		return this.messages[cid][keyOfControlPackets(true, mid)]
	}
	return nil
}

func (this *store) StoreInboundPacket(cid string, p packets.ControlPacket) error {
	return this.storePacket(cid, keyOfControlPackets(true, p.Details().MessageID), p)
}

func (this *store) StoreOutboundPacket(cid string, p packets.ControlPacket) error {
	return this.storePacket(cid, keyOfControlPackets(false, p.Details().MessageID), p)
}

func keyOfControlPackets(inbound bool, mid uint16) string {
	if inbound {
		return fmt.Sprintf("i.%v", mid)
	}
	return fmt.Sprintf("o.%v", mid)
}

func (this *store) storePacket(cid string, key string, message packets.ControlPacket) error {
	switch message.(type){
	case *packets.PublishPacket, *packets.PubrelPacket:
	default:
		return ErrInvalidPacket
	}
	this.Lock()
	defer this.Unlock()
	if _, ok := this.messages[cid]; !ok {
		this.messages[cid] = make(map[string]packets.ControlPacket)
	}
	this.messages[cid][key] = message
	return nil
}

func (this *store) StreamOfflinePackets(cid string, callback func(packets.ControlPacket)) {
	this.RLock()
	defer this.RUnlock()
	if v, ok := this.messages[cid]; ok {
		for key, message := range v {
			if key[0] == 'o' {
				callback(message)
			}
		}
	}
}

func (this *store ) DeleteInboundPacket(cid string, mid uint16) {
	this.deletePacket(cid, keyOfControlPackets(true, mid))
}

func (this *store ) DeleteOutboundPacket(cid string, mid uint16) {
	this.deletePacket(cid, keyOfControlPackets(false, mid))
}

func (this *store) deletePacket(cid string, key string) {
	this.Lock()
	defer this.Unlock()
	if _, ok := this.messages[cid]; ok {
		delete(this.messages[cid], key)
		if len(this.messages[cid]) == 0 {
			delete(this.messages, cid)
		}
	}
}

func (this *store) UpdatePacket(cid string, key string, p packets.ControlPacket) {

}

func (this *store) CleanPackets(cid string) {
	this.Lock()
	defer this.Unlock()
	delete(this.messages, cid)
}

func (this *store ) OfflineMessageLen() int {
	this.RLock()
	defer this.RUnlock()
	count := 0
	for _, value := range this.messages {
		count += len(value)
	}
	return count
}