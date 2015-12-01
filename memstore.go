package mqtt

import (
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"time"
)

type memstore struct {
	messages map[string]map[uint16]*packets.PublishPacket
}

func newMemStore() *memstore {
	store := &memstore{
		messages: make(map[string]map[uint16]*packets.PublishPacket),
	}

	go func() {
		for _ = range time.Tick(time.Second) {
			count := 0
			for _, value := range store.messages {
				count += len(value)
			}
			log.Debug("store messages ", count)
		}
	}()

	return store
}

func (this *memstore) StoreSubscriptions(client *client) {
	if client.clean {
		return
	}
}

func (this *memstore) LookupSubscriptions(client *client) {

}

func (this *memstore) StoreRetained(message *packets.PublishPacket) {

}

func (this *memstore) LookupRetained(topic string, callback func(*packets.PublishPacket)) {

}

func (this *memstore) StoreOfflinePacket(id string, message *packets.PublishPacket) error {
	// only store packet that has subscriptions
	log.Debugf("store off line message msgid %q, cid %q", message.MessageID, id)
	if _, ok := this.messages[id]; !ok {
		this.messages[id] = make(map[uint16]*packets.PublishPacket)
	}
	this.messages[id][message.MessageID] = message
	return nil
}

func (this *memstore) StreamOfflinePackets(id string, callback func(*packets.PublishPacket)) {
	if v, ok := this.messages[id]; ok {
		for _, message := range v {
			go callback(message)
		}
	}
}

func (this *memstore) DeleteOfflinePacket(id string, messageId uint16) {
	if _, ok := this.messages[id]; ok {
		delete(this.messages[id], messageId)
		if len(this.messages[id]) == 0 {
			delete(this.messages, id)
		}
	}
}

func (this *memstore) UpdateOfflinePacket(id string, messageId uint16, message *packets.PublishPacket) {

}

func (this *memstore) CleanOfflinePacket(id string) {
	delete(this.messages, id)
}
