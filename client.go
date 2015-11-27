package mqtt

import (
	"bitbucket.org/j3r0lin/mqtt/connection"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"github.com/dropbox/godropbox/errors"
	"net"
	"sync"
)

type client struct {
	opts    *ServerOpts
	conn    *conn.Connection
	started sync.WaitGroup
	stopped sync.WaitGroup

	server *Server

	mu     sync.Mutex
	closed bool
	stop   chan struct{}
}

func (this *client) start(c net.Conn) error {
	var opts = this.opts
	var conn = conn.NewConnection(c, &conn.Options{
		KeepAlive:      opts.KeepAlive,
		ConnectTimeout: opts.ConnectTimeout,
		AckTimeout:     opts.AckTimeout,
		TimeoutRetries: opts.TimeoutRetries,
	})
	this.conn = conn

	conn.SetConnectHandler(this.handleConnectPacket)

	conn.ConnectionLostHandler = func(err error) {
		this.server.mu.Lock()
		delete(this.server.clients, conn.ClientId)
		this.server.mu.Unlock()
		log.Infoln("client disconnect", err)
	}

	if err := conn.Start(); err != nil {
		conn.Stop()
		return err
	}

	return nil
}

func (this *client) handleConnectPacket(p *packets.ConnectPacket) error {
	//fixme validate username & password

	//fixme init session
	clientId := p.ClientIdentifier

	this.server.mu.Lock()
	if _, ok := this.server.clients[clientId]; ok {
		return errors.New("already connect")
	} else {
		this.server.clients[clientId] = this
	}
	this.server.mu.Unlock()

	log.Infoln("client connect", p.Username, string(p.Password), p.ClientIdentifier)
	return this.conn.Connack(packets.Accepted, false)
}
