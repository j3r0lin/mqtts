package mqtt

import "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"

type Store interface {
	StoreSubscriptions(client *client)
	LookupSubscriptions(client *client)
	StoreRetained(message *packets.PublishPacket)
	LookupRetained(topic string, callback func(*packets.PublishPacket))
	StoreOfflinePacket(id string, message *packets.PublishPacket) error
	StreamOfflinePackets(id string, callback func(*packets.PublishPacket))
	DeleteOfflinePacket(id string, messageId uint16)
	UpdateOfflinePacket(id string, messageId uint16, message *packets.PublishPacket)
	CleanOfflinePacket(id string)
}
