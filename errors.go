package mqtt

import "errors"

var (
	ErrInvalidConnectionType  error = errors.New("mqtt: Invalid connection type")
	ErrInvalidSubscriber      error = errors.New("mqtt: Invalid subscriber")
	ErrBufferNotReady         error = errors.New("mqtt: buffer is not ready")
	ErrBufferInsufficientData error = errors.New("mqtt: buffer has insufficient data.")

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
