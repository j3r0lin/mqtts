package mqtt

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"github.com/Sirupsen/logrus"
	"github.com/astaxie/beego"
	"github.com/stretchr/testify/assert"
)

func init() {
	logrus.StandardLogger().Out = os.Stdout
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetLevel(logrus.DebugLevel)
}

func TestPub(t *testing.T) {
	count := 20
	g := sync.WaitGroup{}
	g.Add(count)
	var connected int32
	beego.NewTree()
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

			atomic.AddInt32(&connected, 1)
			start := time.Now()
			messages := 2
			for _ = range make([]byte, messages) {
				logrus.Debug("publish")
				token = client.Publish("hello", 2, false, "world")
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
	log.Print(uint16(time.Second.Seconds()))
}
