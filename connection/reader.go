package conn

import (
	"errors"

	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"time"
	"io"
	"net"
	"fmt"
)

func (this *Connection) reader() (err error) {
	defer func() {
		log.Debug("reader: stopped, ", err)
		if r := recover(); r != nil {
			log.Warn("reader: panic with", r)
			this.workerExit(fmt.Errorf("reader panic %v", r))
		}
		this.workers.Done()
	}()

	var cp packets.ControlPacket

	for {
		timeout := this.Opts.KeepAlive + (this.Opts.KeepAlive) / 2
		if cp, err = this.readPacket(timeout); err != nil {
			switch err.(type) {
			case net.Error:
				if err.(net.Error).Timeout() {
					log.Debug("reader: client keepalive timeout, ", this.id)
					err = errors.New("keepalive timeout")
				}
			default:
				if err != io.EOF {
					log.Warnf("reader: error(%v) reading from connection (%v)", err, this.id)
				}
			}

			break
		}
		log.Debugf("reader: new packet received, queue len:%v, %q", len(this.in), this.id)

		this.in <- cp
		if _, ok := cp.(*packets.DisconnectPacket); ok {
			return ErrDisconnect
		}
	}
	// We received an error on read.
	// If disconnect is in progress, swallow error and return
	select {
	case <-this.stop:
		return
	// Not trying to disconnect, send the error to the errors channel
	default:
		this.workerExit(err)
		return
	}
}

// read one message from stream
func (this *Connection) readPacket(timeout time.Duration) (cp packets.ControlPacket, err error) {
//	log.Debug("read packet with timeout ", timeout)
	this.conn.SetReadDeadline(time.Now().Add(timeout))
	cp, err = packets.ReadPacket(this.conn)
	this.conn.SetReadDeadline(time.Time{})
	return
}

func (this *Connection) ReadConnectPacket() (p *packets.ConnectPacket, err error) {
	var cp packets.ControlPacket
	var ok bool
	if cp, err = this.readPacket(this.Opts.ConnectTimeout); err == nil {
		if p, ok = cp.(*packets.ConnectPacket); !ok {
			err = errors.New("connect message expected")
		}
	}
	return
}
