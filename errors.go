package mqtt

import "errors"

var (
	ErrInvalidConnectionType  error = errors.New("Invalid connection type")
	ErrInvalidSubscriber      error = errors.New("Invalid subscriber")

	ErrDisconnect              = errors.New("Disconnect")
	ErrRefusedClientId         = errors.New("Refused client id")
	ErrInvalidPacket           = errors.New("Invalid packet")
	ErrInvalidTopicName        = errors.New("Invalid topic name")
	ErrInvalidTopicEmptyString = errors.New("Invalid topic name empty string")
	ErrInvalidTopicMultilevel  = errors.New("Invalid topic multi level")
	ErrInvalidQoS              = errors.New("Invalid QoS")
	ErrTakeOver                = errors.New("Takeover")
	ErrInvalidMessageId        = errors.New("Invalid message id")
)
