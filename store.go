package mqtt

import "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"

type Store interface {
	StoreSubscription(filter, cid string, qos byte)
	DeleteSubscription(filter, cid string)
	CleanSubscription(cid string)
	LookupSubscriptions(callback func(filter, cid string, qos byte))

	StoreRetained(p *packets.PublishPacket)
	LookupRetained(callback func(*packets.PublishPacket))

	FindInboundPacket(cid string, mid uint16) packets.ControlPacket
	StoreInboundPacket(cid string, p packets.ControlPacket) error
	StoreOutboundPacket(cid string, p packets.ControlPacket) error
	StreamOfflinePackets(cid string, callback func(packets.ControlPacket))
	DeleteInboundPacket(cid string, mid uint16)
	DeleteOutboundPacket(cid string, mid uint16)
	CleanPackets(cid string)

	InPacketsSize() int
	OutPacketsSize() int
}
