package reliable_conn

import (
	"fmt"
	"net"
	"sync"
	"time"
	"github.com/sirupsen/logrus"
	"github.com/cenkalti/backoff"
)

var Logger *logrus.Logger



type ReliableConn struct {
	Network string
	Address string
	Backoff backoff.BackOff

	isConnected    bool
	isReconnecting bool
	m              *sync.Mutex
	internal       net.Conn
	q_m            *sync.Mutex
	queue          [][]byte
	dialer         Dialer
}


type Dialer func(network, address string) (net.Conn, error)

func Dial(network, address string) (net.Conn, error) {
    return DialWithDialer(network, address, nil)
}

func DialWithDialer(network, address string, dialer Dialer) (net.Conn, error) {
    if dialer == nil {
        dialer = net.Dial
    }
	return &ReliableConn{
	    Network: network,
	    Address: address,
	    m: new(sync.Mutex),
	    q_m: new(sync.Mutex),
	    dialer: dialer,
	}, nil
}

func (this *ReliableConn) Connect() {
	var cp []byte
	this.m.Lock()
	defer this.m.Unlock()

	if this.Backoff == nil {
		e := backoff.NewExponentialBackOff()

		e.InitialInterval = time.Second
		e.Multiplier = 2
		e.MaxInterval = 10 * time.Second
		e.MaxElapsedTime = 0

		this.Backoff = e
	}

	if this.isConnected || this.isReconnecting {
		return
	}
	this.isReconnecting = true
	this.isConnected = false

	var local_error error = fmt.Errorf("err")
	var new_conn net.Conn
	for local_error != nil {
		new_conn, local_error = this.dialer(this.Network, this.Address)
		if local_error != nil {
			if Logger != nil {
				Logger.Error(fmt.Errorf("reliable_conn: can't connect: %v", local_error))
			}
			time.Sleep(this.Backoff.NextBackOff())
		}
	}
	this.Backoff.Reset()
    if Logger != nil && this.internal != nil {
        Logger.Info("reliable_conn: reconnected")
    }
	this.internal = new_conn
	this.isConnected = true
	this.isReconnecting = false

	this.q_m.Lock()
	defer this.q_m.Unlock()
	for _, elem := range this.queue {
		cp = make([]byte, len(elem))
		copy(cp, elem)
		go this.Write(cp)
	}
	this.queue = [][]byte{}

}

func (this *ReliableConn) Read(b []byte) (n int, err error) {
	if !this.isConnected {
		this.Connect()
	}
	return this.internal.Read(b)
}
func (this *ReliableConn) Write(b []byte) (n int, err error) {
	var orig_err error
	cp := make([]byte, len(b))
	copy(cp, b)
	if this.isConnected {
		n, orig_err = this.internal.Write(cp)
	}
	if !this.isConnected || orig_err != nil {
		if orig_err != nil && Logger != nil {
			Logger.Error(fmt.Errorf("reliable_conn: disconnected: %v", orig_err))
		}

		this.q_m.Lock()
		this.queue = append(this.queue, cp)
		this.q_m.Unlock()
		this.isConnected = false

		go this.Connect()

		return len(b), nil
	}

	return
}

func (this *ReliableConn) Close() error {
	return this.internal.Close()
}

func (this *ReliableConn) LocalAddr() net.Addr {
	return this.internal.LocalAddr()
}

func (this *ReliableConn) RemoteAddr() net.Addr {
	return this.internal.RemoteAddr()
}

func (this *ReliableConn) SetDeadline(t time.Time) error {
	return this.internal.SetDeadline(t)
}

func (this *ReliableConn) SetReadDeadline(t time.Time) error {
	return this.internal.SetReadDeadline(t)
}

func (this *ReliableConn) SetWriteDeadline(t time.Time) error {
	return this.internal.SetWriteDeadline(t)
}
