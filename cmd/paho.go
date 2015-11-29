package main
import (
	"bitbucket.org/j3r0lin/mqtt"
	"github.com/Sirupsen/logrus"
	"os"
	"net/http"
	_ "net/http/pprof"
	"runtime/pprof"
)

func init() {
	logrus.StandardLogger().Out = os.Stdout
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetLevel(logrus.DebugLevel)
}


func main() {
	go func() {
		pprof.Lookup("gorutine")
		logrus.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	server := mqtt.NewServer(&mqtt.ServerOpts{})
	server.ListenAndServe("tcp://localhost:1883")
}
