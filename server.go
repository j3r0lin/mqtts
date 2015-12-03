package mqtt

import (
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"github.com/Sirupsen/logrus"
	"net"
	"net/url"
	"runtime"
	"sync"
	"time"
)

var log = logrus.WithField("module", "mqtt")

type Server struct {
	sync.RWMutex

	opts *Options
	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit chan struct{}

	ln net.Listener

	// A list of services created by the server. We keep track of them so we can
	// gracefully shut them down if they are still alive when the server goes down.
	clients  map[string]*client
	offlines map[string]*client
	store    Store

	subhier *subhier

	// Mutex for updating svcs
}

func NewServer(opts *Options) *Server {
	server := &Server{}
	server.opts = opts

	if opts.KeepAlive == 0 {
		opts.KeepAlive = DefaultKeepAlive
	}

	if opts.ConnectTimeout == 0 {
		opts.ConnectTimeout = DefaultConnectTimeout
	}

	if opts.AckTimeout == 0 {
		opts.AckTimeout = DefaultAckTimeout
	}

	if opts.TimeoutRetries == 0 {
		opts.TimeoutRetries = DefaultTimeoutRetries
	}

	server.quit = make(chan struct{})
	server.clients = make(map[string]*client)
	server.offlines = make(map[string]*client)
	server.subhier = newSubhier()
	server.store = newMemStore()

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
		log.Infoln("gorutines", runtime.NumGoroutine(), "clients", len(this.clients))
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

func (this *Server) storePacket(message *packets.PublishPacket) {
	if message.Retain {
		retain(message)
	}

	if message.Qos != 0 {
		l, err := this.subhier.search(message.TopicName, message.Qos)
		if err != nil {
			return
		}
		log.Debugf("store offline packet, found subs count %v", l.Len())

		for e := l.Front(); e != nil; e = e.Next() {
			if sub, ok := e.Value.(*subscribe); ok {
				cli := sub.client
				log.Debugf("sotre offline packet to %q", cli.id)
				this.store.StoreOfflinePacket(cli.id, message)
			}
		}
	}
}

func (this *Server) deleteOfflinePacket(clientId string, messageId uint16) {
	this.store.DeleteOfflinePacket(clientId, messageId)
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

//			if _, ok := published[cli.id]; ok {
//				continue
//			}
//			published[cli.id] = true

			if c, ok := this.clients[cli.id]; ok && !c.closed {
				log.Debugf("forward message to %q, topic: %q, qos: %q, msgid: %q ", cli.id, message.TopicName, qos, message.MessageID)
				cli.publish(message.TopicName, message.Payload, qos, message.MessageID, message.Retain)
			}
		}
	}
}

func (this *Server) forwardOfflineMessage(c *client) {
	if c.clean {
		return
	}
	log.Infof("forward offline message of %q", c.id)
	this.store.StreamOfflinePackets(c.id, func(message *packets.PublishPacket) {
		log.Debugf("forward offline message to %q, topic: %q, qos: %q, msgid: %q ", c.id, message.TopicName, message.Qos, message.MessageID)
		c.publish(message.TopicName, message.Payload, message.Qos, message.MessageID, message.Retain)
	})
}

func (this *Server) cleanSeassion(c *client) {
	log.Debugf("clean session of %q", c.id)
	this.subhier.clean(c)
	this.store.CleanOfflinePacket(c.id)
}
