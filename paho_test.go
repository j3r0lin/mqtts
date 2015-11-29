package mqtt

import (
	"os"
	"sync"
	"testing"
	"time"

	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"strings"
	"code.google.com/p/go-uuid/uuid"
	"runtime"
)

func init() {
	logrus.StandardLogger().Out = os.Stdout
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetLevel(logrus.DebugLevel)
}

func TestPub(t *testing.T) {
	count := 10
	g := sync.WaitGroup{}
	g.Add(count)
	server := NewServer(&ServerOpts{})
	go func() {
		err := server.ListenAndServe("tcp://localhost:1883")
		assert.NoError(t, err)
	}()

	time.Sleep(time.Second)
	//	runtime.GOMAXPROCS(1)
	//	mqtt.ERROR = glog.New(os.Stderr, "", 0)
	//	mqtt.CRITICAL = glog.New(os.Stderr, "", 0)
	//	mqtt.WARN = glog.New(os.Stderr, "", 0)
	//	mqtt.DEBUG = glog.New(os.Stderr, "", 0)
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

			token = client.Subscribe("a/b/d", 0, func(cli *mqtt.Client, msg mqtt.Message) {
				log.Println("message received ", msg.Topic(), string(msg.Payload()))
			})

			token.Wait()
			assert.NoError(t, token.Error(), "sub failed")

			start := time.Now()
			messages := 2
			for _ = range make([]byte, messages) {
				logrus.Debug("publish")
				token = client.Publish("a/b/c", 2, false, "world")
				token.Wait()
				assert.NoError(t, token.Error(), "publish")
				time.Sleep(time.Second)
			}
			client.Disconnect(3000)
			log.Infoln(float64(messages) / time.Now().Sub(start).Seconds())
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
	for i := 0; i< 100000; i++ {
		id := uuid.New()
		if i == 1 {
			search = id
		}
		m[id] = uuid.New()
	}

	start := time.Now()
	for i := 0; i< 1000000; i++ {
		_ = m[search]
	}
	log.Println(float64(1000000) / time.Now().Sub(start).Seconds())

	stat := runtime.MemStats{}
	runtime.ReadMemStats(&stat)
	log.Println(stat.TotalAlloc, stat.Frees)
}