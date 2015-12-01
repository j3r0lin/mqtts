package mqtt
import (
	"testing"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"strings"
	"time"
)




func TestRetain(t *testing.T) {
	count := 10
	start := time.Now()
	message := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	message.Payload = []byte("12341234")
	message.TopicName = "a/b/c"
	retain(message)
	for _ = range strings.Repeat(" ", count) {
		message.TopicName = "a/b/d"
		retain(message)
	}
	log.Println("speed", float64(count) / time.Now().Sub(start).Seconds())

	message.TopicName = "c/b/d"
	retain(message)

	message.TopicName = "a/b/c"
	message.Payload = nil
	retain(message)

	defaultRetains.print(0)

	matchRetain("+/+/d", func(message *packets.PublishPacket) {
		log.Println(message)
	})
}