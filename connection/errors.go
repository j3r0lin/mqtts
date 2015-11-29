package conn
import "errors"

var (
	ErrDisconnect = errors.New("Disconnect")
	ErrMessageIdInvalid = errors.New("message id must > 0")
)
