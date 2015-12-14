package mqtt
import (
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"bytes"
)

func MarshalPacket(p packets.ControlPacket) []byte {
	var w bytes.Buffer
	p.Write(&w)
	return w.Bytes()
}

func UnmarshalPacket(p []byte) packets.ControlPacket {
	r := bytes.NewBuffer(p)
	cp, _ := packets.ReadPacket(r)
	return cp
}

