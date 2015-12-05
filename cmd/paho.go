package main
import (
	"bitbucket.org/j3r0lin/mqtt"
	"github.com/Sirupsen/logrus"
	"os"
	_ "net/http/pprof"
	"sync"
)

func init() {
	logrus.StandardLogger().Out = os.Stdout
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetLevel(logrus.DebugLevel)
}


func main() {

	wg := sync.WaitGroup{}
	wg.Add(1)
	server := mqtt.NewServer(mqtt.NewOptions())
	go func() {
		logrus.Fatal(server.ListenAndServe("tcp://0.0.0.0:1883"))
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		logrus.Fatal(server.ListenAndServeWebSocket(":8080"))
		wg.Done()
	}()

	wg.Wait()
}
