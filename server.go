package mqtt

import (
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"runtime"
)

var log = logrus.WithField("module", "mqtt")

type Server struct {
	opts *ServerOpts
	// The quit channel for the server. If the server detects that this channel
	// is closed, then it's a signal for it to shutdown as well.
	quit chan struct{}

	ln net.Listener

	// A list of services created by the server. We keep track of them so we can
	// gracefully shut them down if they are still alive when the server goes down.
	clients map[string]*client

	// Mutex for updating svcs
	mu sync.Mutex
}

func NewServer(opts *ServerOpts) *Server {
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

func (this *Server) handleConnection(conn net.Conn) (cli *client, err error) {
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	if conn == nil {
		return nil, ErrInvalidConnectionType
	}

	cli = &client{
		server: this,
		opts:   this.opts,
	}

	if err = cli.start(conn); err != nil {
		return nil, err
	}

	return nil, nil
}
