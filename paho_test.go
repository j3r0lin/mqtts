package mqtt

import (
	"os"
	"sync"
	"testing"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net/http"
	"runtime"
	_ "net/http/pprof"
	"runtime/pprof"
	"strings"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
)

func init() {
	logrus.StandardLogger().Out = os.Stdout
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetLevel(logrus.DebugLevel)

}

func prepare() {
	server := NewServer(NewOptions())
	go func() {
		pprof.Lookup("gorutine")
		logrus.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()
	go func() {
		err := server.ListenAndServe("tcp://localhost:1883")
		if err != nil {
			logrus.Fatal(err)
		}
	}()
}

func TestDupConnect(t *testing.T) {
	//	prepare()
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://localhost:1883")
	opts.SetClientID("123")
	opts.AutoReconnect = false
	opts.SetConnectionLostHandler(func(c *mqtt.Client, err error) {
		logrus.Debug(err)
	})

	client1 := mqtt.NewClient(opts)
	client2 := mqtt.NewClient(opts)
	token := client1.Connect()
	token.Wait()
	assert.NoError(t, token.Error())

	token = client2.Connect()
	token.Wait()
	assert.NoError(t, token.Error())

	time.Sleep(time.Second)
	token = client1.Publish("topic", 2, false, "hello")
	token.Wait()
	assert.Error(t, token.Error())
	assert.False(t, client1.IsConnected())

}

func TestPubBench(t *testing.T) {
	//	prepare()
	count := 1
	g := sync.WaitGroup{}
	g.Add(count)

	time.Sleep(time.Second)

	for i := 0; i < count; i++ {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Errorln("panic ", err)
				}
				g.Done()
			}()
			opts := mqtt.NewClientOptions()
			opts.AddBroker("tcp://localhost:1883")
			//			opts.SetClientID("123")
			opts.SetUsername("user")
			opts.SetPassword("pwd")
			opts.SetKeepAlive(5 * time.Second)
			opts.SetAutoReconnect(false)

			opts.SetWill("/topic/will", "will message", 0, false)
			client := mqtt.NewClient(opts)

			CONN:
			token := client.Connect()
			token.Wait()
			if token.Error() != nil {
				log.Info("connect failed", token.Error())
				time.Sleep(time.Second)
				goto CONN
			}

			log.Infoln("client connected", opts.ClientID)

			token = client.Subscribe("#", 0, func(cli *mqtt.Client, msg mqtt.Message) {
				log.Println("message received ", msg.Topic(), string(msg.Payload()))
			})

			token.Wait()
			assert.NoError(t, token.Error(), "sub failed")

			start := time.Now()
			messages := 2
			for _ = range make([]byte, messages) {
				logrus.Debug("publish")
				token = client.Publish("", 2, false, "world")
				token.Wait()
				assert.NoError(t, token.Error(), "publish")
				time.Sleep(time.Second)
			}
			client.Disconnect(3000)
			log.Infoln(float64(messages) / time.Since(start).Seconds())
		}()
	}

	g.Wait()
	time.Sleep(10 * time.Second)
}

func TestConnectPacket(t *testing.T) {
	log.Println(len(strings.Split("/", "/")))
	log.Print(uint16(time.Second.Seconds()))
}

func TestMapSearch(t *testing.T) {

	m := make(map[string]string, 10000)

	var search string
	for i := 0; i < 100000; i++ {
		id := uuid.New()
		if i == 1 {
			search = id
		}
		m[id] = uuid.New()
	}

	start := time.Now()
	for i := 0; i < 1000000; i++ {
		_ = m[search]
	}
	log.Println(float64(1000000) / time.Now().Sub(start).Seconds())

	stat := runtime.MemStats{}
	runtime.ReadMemStats(&stat)
	log.Println(stat.TotalAlloc, stat.Frees)
}

func BenchmarkPub(b *testing.B) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://localhost:1883")
	opts.SetClientID("clientid")
	//	opts.AutoReconnect = false
	opts.SetConnectionLostHandler(func(c *mqtt.Client, err error) {
		logrus.Debug(err)
	})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	assert.NoError(b, token.Error())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token := client.Publish("a/b/c", 2, false, "message payload")
		token.Wait()
	}
}

func BenchmarkMarshalPacket(b *testing.B) {
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = "a/b/c"
	p.Payload = []byte("hello world")
	for i := 0; i < b.N; i++ {
		MarshalPacket(p)
	}
}

func BenchmarkUnmarshalPacket(b *testing.B) {
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = "a/b/c"
	p.Payload = []byte("hello world")
	value := MarshalPacket(p)
	for i := 0; i < b.N; i++ {
		UnmarshalPacket(value)
	}
}


func TestRetainPubSub(t *testing.T) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://localhost:1883")
	//	opts.SetClientID("123")
	opts.AutoReconnect = false
	opts.SetConnectionLostHandler(func(c *mqtt.Client, err error) {
		logrus.Debug(err)
	})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	assert.NoError(t, token.Error())

	defer client.Disconnect(10)
	token = client.Publish("topic/retain", 1, true, "retain message")
	token.Wait()
	assert.NoError(t, token.Error())

	retained := 0
	client.Subscribe("topic/retain", 1, func(c *mqtt.Client, m mqtt.Message) {
		if m.Retained() {
			assert.Equal(t, m.Topic(), "topic/retain")
			assert.Equal(t, m.Payload(), []byte("retain message"))
			retained++
		}
	})

	time.Sleep(time.Second)
	assert.Equal(t, retained, 1)

	client.Unsubscribe("topic/retain").Wait()

	client.Publish("topic/retain", 1, true, "").Wait()

	retained = 0
	client.Subscribe("topic/retain", 1, func(c *mqtt.Client, m mqtt.Message) {
		log.Debugln(m.Topic(), string(m.Payload()))
		if m.Retained() {
			retained++
		}
	})

	time.Sleep(time.Second)
	assert.Equal(t, retained, 0)


}


func TestPubSub(t *testing.T) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://localhost:1883")
	opts.SetClientID("clientid")
	opts.AutoReconnect = true
	opts.CleanSession = false

	opts.SetConnectionLostHandler(func(c *mqtt.Client, err error) {
		logrus.Debug(err)
	})

	client := mqtt.NewClient(opts)
	client.Connect().Wait()

	client.Subscribe("a/b/c", 2, func(c *mqtt.Client, m mqtt.Message) {
		log.Debug(string(m.Payload()))
	}).Wait()

	go func() {
		for _ = range time.Tick(time.Second) {
			go client.Publish("a/b/c", 2, false, time.Now().String()).Wait()
		}
	}()

	time.Sleep(100 * time.Second)

}