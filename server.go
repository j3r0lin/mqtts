package mqtt

import (
	"net"
	"net/url"
	"runtime"
	"sync"
	"time"

	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"github.com/Sirupsen/logrus"
	"reflect"
)

var log = logrus.StandardLogger()

type Server struct {
	sync.RWMutex

	opts     *Options
	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit     chan struct{}

	ln       net.Listener

	// A list of services created by the server. We keep track of them so we can
	// gracefully shut them down if they are still alive when the server goes down.
	clients  *clients
	offlines map[string]*client
	store    *store

	subhier  *subhier
	mids     *messageIds

	// Mutex for updating svcs
}

func NewServer(opts *Options) *Server {
	server := &Server{}
	server.opts = opts

	server.quit = make(chan struct{})
	server.clients = newClients()
	server.subhier = newSubhier()
	server.store = newStore()
	server.mids = newMessageIds()

	return server
}

func (this *Server) ListenAndServe(uri string) error {
	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	this.ln, err = net.Listen(u.Scheme, u.Host)
	if err != nil {
		return err
	}
	defer this.ln.Close()

	log.Info("MQTT server listenning on ", uri)
	go this.stat()

	var tempDelay time.Duration // how long to sleep on accept failure
	for {
		conn, err := this.ln.Accept()

		if err != nil {
			select {
			case <-this.quit:
				return nil
			default:
			}
			// see http server
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Errorf("mqtt: Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		go this.handleConnection(conn)
	}
	return nil
}

func (this *Server) stat() error {
	for range time.Tick(time.Second) {
		log.Infof("gorutines: %v, clients: %v, store: %v", runtime.NumGoroutine(), this.clients.len(), this.store.OfflineMessageLen())
	}
	return nil
}

func (this *Server) handleConnection(conn net.Conn) (c *client, err error) {
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	if conn == nil {
		return nil, ErrInvalidConnectionType
	}

	c = &client{
		server: this,
		opts:   this.opts,
		conn:   conn,
	}

	if err = c.start(); err != nil {
		return nil, err
	}

	return nil, nil
}

func (this *Server) deleteOfflinePacket(clientId string, messageId uint16) {
}

func (this *Server) forwardMessage(message *packets.PublishPacket) {
	l, err := this.subhier.search(message.TopicName, message.Qos)
	if err != nil {
		return
	}

	log.Debugf("forward message %v to topic %q, clients: %v", message.MessageID, message.TopicName, l.Len())

	//	published := make(map[string]bool)
	for e := l.Front(); e != nil; e = e.Next() {
		if sub, ok := e.Value.(*subscribe); ok {

			cli := sub.client
			qos := sub.qos

			if _, ok := this.clients.get(cli.id); ok {
				log.Debugf("forward message to %q, topic: %q, qos: %q", cli.id, message.TopicName, qos)
				// It MUST set the RETAIN flag to 0 when a PUBLISH Packet is sent to a Client
				// because it matches an established subscription regardless of
				// how the flag was set in the message it received.
				cli.publish(message.TopicName, message.Payload, qos, false, message.Dup)
				return
			}
			if qos > 0 {
				p := message.Copy()
				p.Qos = qos
				p.Retain = false
				p.Dup = message.Dup
				p.MessageID = this.mids.request(cli.id)
				this.store.StoreOutboundPacket(cli.id, p)
			}
		}
	}
}

func (this *Server) forwardOfflineMessage(c *client) {
	if c.clean {
		return
	}
	log.Infof("forward offline message of %q", c.id)
	this.store.StreamOfflinePackets(c.id, func(p packets.ControlPacket) {
		log.Debugf("forward offline message to %q, type: %v, mid: %v", c.id, reflect.TypeOf(p), p.Details().MessageID)
		c.write(p)
	})
}

func (this *Server) cleanSession(c *client) {
	log.Debugf("clean session of %q", c.id)
	this.subhier.clean(c)
	this.mids.clean(c.id)
	this.store.CleanPackets(c.id)
}
