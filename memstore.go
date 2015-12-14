package mqtt

import (
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"sync"
)

type MemoryStore struct {
	sync.RWMutex
	messages map[string]map[string]packets.ControlPacket
}

func newMemoryStore() *MemoryStore {
	store := &MemoryStore{
		messages: make(map[string]map[string]packets.ControlPacket),
	}
	return store
}

func (this *MemoryStore) StoreSubscriptions(client *client) {
	if client.clean {
		return
	}
}

func (this *MemoryStore) LookupSubscriptions(client *client) {

}

func (this *MemoryStore) StoreRetained(message *packets.PublishPacket) {

}

func (this *MemoryStore) LookupRetained(topic string, callback func(*packets.PublishPacket)) {

}

func (this *MemoryStore) FindInboundPacket(cid string, mid uint16) packets.ControlPacket {
	this.Lock()
	defer this.Unlock()
	if _, ok := this.messages[cid]; ok {
		return this.messages[cid][keyOfControlPackets(true, mid)]
	}
	return nil
}

func (this *MemoryStore) StoreInboundPacket(cid string, p packets.ControlPacket) error {
	return this.storePacket(cid, keyOfControlPackets(true, p.Details().MessageID), p)
}

func (this *MemoryStore) StoreOutboundPacket(cid string, p packets.ControlPacket) error {
	return this.storePacket(cid, keyOfControlPackets(false, p.Details().MessageID), p)
}

func keyOfControlPackets(inbound bool, mid uint16) string {
	if inbound {
		return fmt.Sprintf("i.%v", mid)
	}
	return fmt.Sprintf("o.%v", mid)
}

func (this *MemoryStore) storePacket(cid string, key string, message packets.ControlPacket) error {
	switch message.(type) {
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

func (this *MemoryStore) StreamOfflinePackets(cid string, callback func(packets.ControlPacket)) {
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

func (this *MemoryStore) DeleteInboundPacket(cid string, mid uint16) {
	this.deletePacket(cid, keyOfControlPackets(true, mid))
}

func (this *MemoryStore) DeleteOutboundPacket(cid string, mid uint16) {
	this.deletePacket(cid, keyOfControlPackets(false, mid))
}

func (this *MemoryStore) deletePacket(cid string, key string) {
	this.Lock()
	defer this.Unlock()
	if _, ok := this.messages[cid]; ok {
		delete(this.messages[cid], key)
		if len(this.messages[cid]) == 0 {
			delete(this.messages, cid)
		}
	}
}

func (this *MemoryStore) UpdatePacket(cid string, key string, p packets.ControlPacket) {

}

func (this *MemoryStore) CleanPackets(cid string) {
	this.Lock()
	defer this.Unlock()
	delete(this.messages, cid)
}

func (this *MemoryStore) OfflineMessageLen() uint64 {
	this.RLock()
	defer this.RUnlock()
	var count uint64 = 0
	for _, value := range this.messages {
		count += uint64(len(value))
	}
	return count
}
