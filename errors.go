package mqtt

import "errors"

var (
	ErrInvalidConnectionType  error = errors.New("mqtt: Invalid connection type")
	ErrInvalidSubscriber      error = errors.New("mqtt: Invalid subscriber")
	ErrBufferNotReady         error = errors.New("mqtt: buffer is not ready")
	ErrBufferInsufficientData error = errors.New("mqtt: buffer has insufficient data.")

	ErrDisconnect       = errors.New("Disconnect")
	ErrRefusedClientId  = errors.New("refused client id")
	ErrBadPacket        = errors.New("Bad packet")
	ErrTakeOver         = errors.New("Takeover")
	ErrMessageIdInvalid = errors.New("message id must > 0")
)
