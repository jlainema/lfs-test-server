package main

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"time"
)

const (
	contentMediaType = "application/vnd.git-lfs"
	metaMediaType    = contentMediaType + "+json"
	version          = "0.4.0"
)

var (
	logger = NewKVLogger(os.Stdout)
)

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}

func wrapHttps(l net.Listener, cert, key string) (net.Listener, error) {
	var err error

	config := &tls.Config{}

	if config.NextProtos == nil {
		config.NextProtos = []string{"http/1.1"}
	}

	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}

	netListener := l.(*TrackingListener).Listener

	tlsListener := tls.NewListener(tcpKeepAliveListener{netListener.(*net.TCPListener)}, config)
	return tlsListener, nil
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "-v" {
		fmt.Println(version)
		os.Exit(0)
	}

	// startup all servers from data
	files, err := ioutil.ReadDir("data")

	if err != nil {
		logger.Fatal(kv{"err": err})
	}

	ch := make(chan string)
	for _, f := range files {
		if f.IsDir() {
			logger.Log(kv{"dir": f.Name()})
			go newserver(f.Name(), ch)
			logger.Log(kv{"state": <-ch})
		}
	}

	for {
		time.Sleep(time.Second)
	}
}
