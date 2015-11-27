package conn

import (
	"errors"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"io"
	"net"
	"sync"
	"time"
	"gopkg.in/mgo.v2/bson"
)

var log = logrus.WithField("module", "connection")

type Connection struct {
	id                    string
	ClientId              string
	conn                  net.Conn
	stop                  chan struct{}
	errors                chan error
	workers               sync.WaitGroup

	Opts                  *Options

	ConnectionLostHandler ConnectionLostHandler
	PacketHandler         PacketHandler
	ConnectHandler        ConnectHandler

	in                    chan packets.ControlPacket
	out                   chan packets.ControlPacket
}

type ConnectionLostHandler func(error)
type PacketHandler func(packets.ControlPacket) error
type ConnectHandler func(*packets.ConnectPacket) error

func NewConnection(conn net.Conn, opts *Options) *Connection {
	return &Connection{
		id:     uuid.New(),
		conn:   conn,
		Opts:   opts,
		errors: make(chan error),
		stop:   make(chan struct{}),
	}
}

func (this *Connection) SetPacketHandler(handler PacketHandler) {
	this.PacketHandler = handler
}
func (this *Connection) SetConnectHandler(handler ConnectHandler) {
	this.ConnectHandler = handler
}

func (this *Connection) Start() (err error) {

	if err = this.waitConnect(); err != nil {
		return
	}

	this.in = make(chan packets.ControlPacket, 10)
	this.out = make(chan packets.ControlPacket, 10)

	this.workers.Add(1)
	go this.reader()

	this.workers.Add(1)
	go this.writer()

	this.workers.Add(1)
	go this.process()

	return nil
}

func (this *Connection) waitConnect() (err error) {
	var cp *packets.ConnectPacket

	if cp, err = this.ReadConnectPacket(); err != nil {
		return
	}

	if code := cp.Validate(); code != packets.Accepted {
		this.Connack(code, false)
		return fmt.Errorf("auth failed %d", code)
	}

	if len(cp.ClientIdentifier) == 0 {
		cp.ClientIdentifier = bson.NewObjectId().Hex()
	}

	this.ClientId = cp.ClientIdentifier

	if err = this.ConnectHandler(cp); err != nil {
		if _, ok := err.(net.Error); !ok {
			this.Connack(packets.ErrRefusedBadUsernameOrPassword, false)
		}
		return err
	}

	if cp.KeepaliveTimer != 0 {
		this.Opts.KeepAlive = time.Duration(cp.KeepaliveTimer) * time.Second
	}
	return nil
}

func (this *Connection) Stop() {
	this.errors <- errors.New("shutdown")
}

func (this *Connection) close(err error) {
	log.Debug("closing connection, ", err)
	this.conn.Close()
	close(this.stop)
	this.workers.Wait()

	if err == io.EOF || err == io.ErrUnexpectedEOF {
		log.Info("client closed the connection")
	} else {
		log.Info("client connection closed, ", err)
	}
	if this.ConnectionLostHandler != nil {
		this.ConnectionLostHandler(err)
	}
}
