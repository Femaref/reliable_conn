package reliable_conn

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
	"crypto/tls"
	"github.com/Sirupsen/logrus"
	"os"
)

func TestMain(m *testing.M) {
	Logger = logrus.New()
	os.Exit(m.Run())
}

type Listener func(network, laddr string) (net.Listener, error)

func listenerForTest(port string, expected_total_conns int) (string, net.Listener, func(string) bool, error) {
    return listenerForTestWithListener(port, expected_total_conns, nil)
}

func listenerForTestWithListener(port string, expected_total_conns int, l Listener) (string, net.Listener, func(string) bool, error) {
    if l == nil {
        l = net.Listen
    }
	host := fmt.Sprintf("127.0.0.1:%s", port)
	listener, err := l("tcp", host)
	if err != nil {
		return "", nil, nil, err
	}

	_, port, err = net.SplitHostPort(listener.Addr().String())

	if err != nil {
		listener.Close()
		return "", nil, nil, err
	}

	m := new(sync.Mutex)
	wait_for := make(map[string]bool)
	conns := 0
	go func() {
		for {
			// Wait for a connection.
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go func(c net.Conn) {
				defer c.Close()
				m.Lock()
				conns++
				if conns > expected_total_conns {
					fmt.Println("more than expected conns")
					listener.Close()
					return
				}
				m.Unlock()

				defer func() {
					m.Lock()
					conns--
					m.Unlock()
				}()
				for {
					var err error
					var buffer []byte
					buffer, err = waitFor(c)
					if err != nil {
						return
					}
					s := string(buffer)
					results := strings.Split(s, "\n")
					m.Lock()
					for _, elem := range results {
						wait_for[elem] = true
					}
					m.Unlock()

					_, err = c.Write(buffer)
					if err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return port, listener, func(s string) bool { val, ok := wait_for[s]; return ok && val }, nil
}

func waitFor(conn net.Conn) (buffer []byte, err error) {
	var bytes_read int

	buffer = make([]byte, 1024)
	for bytes_read == 0 {
		bytes_read, err = conn.Read(buffer)
		if err != nil {
			return nil, err
		}

		time.Sleep(250 * time.Millisecond)
	}

	return buffer[:bytes_read], nil
}

func TestListener(t *testing.T) {
	var err error
	port, listener, did_receive, err := listenerForTest("0", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	host := fmt.Sprintf("127.0.0.1:%s", port)
	conn, err := net.Dial("tcp", host)

	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	to_send := "ping"
	_, err = fmt.Fprintf(conn, to_send)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := waitFor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if to_send != string(buffer) {
		t.Fatal("buffer not equal to to_send")
	}

	if !did_receive(to_send) {
		t.Fatal("listener did not receive")
	}
}

func TestNormalOperation(t *testing.T) {
	var err error
	port, listener, _, err := listenerForTest("0", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	host := fmt.Sprintf("127.0.0.1:%s", port)
	conn, err := Dial("tcp", host)

	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	to_send := "ping"
	_, err = fmt.Fprintf(conn, to_send)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := waitFor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if to_send != string(buffer) {
		t.Fatal("buffer not equal to to_send")
	}
}

func TestMonkeyRead(t *testing.T) {
	var err error
	port, listener, _, err := listenerForTest("0", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	host := fmt.Sprintf("127.0.0.1:%s", port)
	conn, err := Dial("tcp", host)

	if err != nil {
		t.Fatal(err)
	}
	rc := conn.(*ReliableConn)
	rc.Connect()

	rc.internal.Close()
	buffer := make([]byte, 1024)
	_, err = rc.Read(buffer)
}

func TestMonkeyWrite(t *testing.T) {
	var err error
	port, listener, did_receive, err := listenerForTest("0", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	host := fmt.Sprintf("127.0.0.1:%s", port)
	conn, err := Dial("tcp", host)

	if err != nil {
		t.Fatal(err)
	}
	rc := conn.(*ReliableConn)
	rc.Connect()
	fmt.Fprintf(conn, "before_close")
	rc.internal.Close()

	time.Sleep(1000 * time.Millisecond)

	to_send := []string{"ping", "pong", "pow"}
	for _, elem := range to_send {
		fmt.Fprintf(conn, fmt.Sprintf("%s\n", elem))
	}

	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2000 * time.Millisecond)

	for _, elem := range to_send {
		if !did_receive(elem) {
			t.Errorf("listener did not receive %s", elem)
		}
	}
}

func TestTLS(t *testing.T) {
    cer, err := tls.LoadX509KeyPair("testdata/server.crt", "testdata/server.key")
    if err != nil {
        t.Fatal(err)
    }

    config := &tls.Config{Certificates: []tls.Certificate{cer}}

    l := func(network, laddr string) (net.Listener, error) {
        return tls.Listen(network, laddr, config)
    }

    d := func(network, address string) (net.Conn, error) {
        config := &tls.Config{InsecureSkipVerify:true}
        return tls.Dial(network, address, config)
    }

    port, listener, _, err := listenerForTestWithListener("0", 1, l)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	host := fmt.Sprintf("127.0.0.1:%s", port)
	conn, err := DialWithDialer("tcp", host, d)

	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	to_send := "ping"
	_, err = fmt.Fprintf(conn, to_send)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := waitFor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if to_send != string(buffer) {
		t.Fatal("buffer not equal to to_send")
	}
}
