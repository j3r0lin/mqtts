package mqtt

import "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"

type Persist interface {
	StoreSubscriptions(client *client)
	LookupSubscriptions(client *client)
	StoreRetained(message *packets.PublishPacket)
	LookupRetained(topic string, callback func(*packets.PublishPacket))
	StoreOfflinePacket(id string, in bool, message packets.ControlPacket) error
	StreamOfflinePackets(id string, in bool, callback func(packets.ControlPacket))
	DeleteOfflinePacket(id string, in bool, messageId uint16)
	UpdateOfflinePacket(id string, in bool, messageId uint16, message packets.ControlPacket)
	CleanOfflinePacket(id string)
}
