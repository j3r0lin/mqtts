package mqtt

import (
	"errors"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"io"
	"net"
	"reflect"
	"time"
)

func (this *client) reader() (err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			err = fmt.Errorf("reader panic %v", r)
		}
		log.Debugf("reader(%v) stopped, %v, %v", this.id, err, this.Err())
		go this.close()
	}()

	var cp packets.ControlPacket

	for {
		timeout := this.keepAlive + (this.keepAlive)/2
		if cp, err = this.readPacket(timeout); err != nil {
			switch err.(type) {
			case net.Error:
				if err.(net.Error).Timeout() {
					log.Debugf("reader(%v) client keepalive timeout, ", this.id)
					err = errors.New("keepalive timeout")
				}
			default:
				if err != io.EOF {
					log.Warnf("reader(%v) error(%v) reading from connection", this.id, err)
				}
			}

			break
		}
		log.Debugf("reader(%v) new packet received, %v, queue len:%v", this.id, reflect.TypeOf(cp), len(this.in))

		select {
		case <-this.Dying():
			return
		case this.in <- cp:
		}
	}
	return
}

// read one message from stream
func (this *client) readPacket(timeout time.Duration) (cp packets.ControlPacket, err error) {
	//	log.Debug("read packet with timeout ", timeout)
	this.conn.SetReadDeadline(time.Now().Add(timeout))
	cp, err = packets.ReadPacket(this.conn)
	this.conn.SetReadDeadline(time.Time{})
	return
}

func (this *client) ReadConnectPacket() (p *packets.ConnectPacket, err error) {
	var cp packets.ControlPacket
	var ok bool
	if cp, err = this.readPacket(this.opts.ConnectTimeout); err == nil {
		if p, ok = cp.(*packets.ConnectPacket); !ok {
			err = errors.New("connect message expected")
		}
	}
	return
}
