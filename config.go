package main

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"syscall"
)

// Configuration holds application configuration. Values will be pulled from
// environment variables, prefixed by keyPrefix. Default values can be added
// via tags.
type Configuration struct {
	Listen      string `config:"tcp://:"`
	Host        string `config:"localhost"`
	Port        string `config:"0"`
	MetaDB      string `config:".db"`
	Size        string `config:"2000000000"`
	ContentPath string `config:"root"`
	Server      string `config:"root"`
	AdminPass   string `config:"admin"`
	ReaderPass  string `config:"reader"`
	Cert        string `config:""`
	Key         string `config:""`
	Scheme      string `config:"http"`
	Public      string `config:"public"`
	UseTus      string `config:"false"`
	TusHost     string `config:"localhost:1080"`
	Up          string `config:"down"`
}

func (c *Configuration) IsHTTPS() bool {
	return strings.Contains(c.Scheme, "https")
}

func (c *Configuration) IsPublic() bool {
	switch c.Public {
	case "1", "true", "TRUE":
		return true
	}
	return false
}

func (c *Configuration) IsUsingTus() bool {
	switch c.UseTus {
	case "1", "true", "TRUE":
		return true
	}
	return false
}

func (c *Configuration) checkBasicAuth(pass string, ok bool) int {
	logger.Log(kv{"server": c.Server, "pass": pass})
	if !ok {
		return 0
	}
	if pass == c.ReaderPass {
		return 1
	}
	if pass == c.AdminPass {
		return 2
	}
	return 0
}

// Config is the global app configuration map from server to config
var Config map[string]*Configuration = map[string]*Configuration{}

const keyPrefix = "LFS"

func rstr(count int) string {
	rnd := make([]byte, count)
	if _, err := rand.Read(rnd); err != nil {
		panic("no random source: " + err.Error())
	}
	dst := make([]byte, count)
	for i, c := range rnd {
		dst[i] = 'A' + (c & 63)
	}
	return string(dst)
}

func newserver(server string) *Configuration {
	// check if server exists; if so, return it
	s, ok := Config[server]
	if ok {
		return s
	}
	// does not exist; we are setting this one up
	s = &Configuration{}
	Config[server] = s
	var local map[string]string = map[string]string{}
	local["LFS_SERVER"] = server

	// if it does not exist on disk, create folder for it (will launch on random port)
	sd := "data/" + server
	cfn := sd + "/.c"
	if _, err := os.Stat(sd); os.IsNotExist(err) {
		if err := os.MkdirAll(sd, 0750); err != nil {
			logger.Fatal(kv{"fn": "newserver", "err": "Could not create folder: " + err.Error()})
			return s
		}
		local["LFS_PORT"] = "0"
		local["LFS_LISTEN"] = "tcp://:"
		local["LFS_ADMINPASS"] = "w_" + rstr(16)
		local["LFS_READERPASS"] = "r_" + rstr(16)
		local["LFS_METADB"] = sd + "/.db"
		local["LFS_CONTENTPATH"] = sd
		if host, err := os.Hostname(); err == nil {
			local["LFS_HOST"] = host
		}
	}

	if file, err := os.Open(cfn); err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			arr := strings.SplitN(scanner.Text(), "=", 1)
			if len(arr) == 2 {
				logger.Log(kv{"name": arr[0], "value": arr[1]})
				local[arr[0]] = arr[1]
			}
		}
	}

	te := reflect.TypeOf(s).Elem()
	ve := reflect.ValueOf(s).Elem()

	for i := 0; i < te.NumField(); i++ {
		sf := te.Field(i)
		name := sf.Name
		field := ve.FieldByName(name)

		envVar := strings.ToUpper(fmt.Sprintf("%s_%s", keyPrefix, name))
		env, ok := local[envVar]
		if !ok {
			env = os.Getenv(envVar)
			if env == "" {
				env = sf.Tag.Get("config")
			}
			logger.Log(kv{"mode": "env", "name": envVar, "value": env})
		} else {
			logger.Log(kv{"mode": "local", "name": envVar, "value": env})
		}
		field.SetString(env)
	}

	var listener net.Listener
	var ls string
	var tl *TrackingListener
	var err error
	for {
		ls = s.Listen
		port := s.Port
		if port == "0" {
			dst := make([]byte, 2)
			rand.Read(dst)
			port = strconv.Itoa(int(dst[0]) + int(dst[1])*256)
		}
		ls += port
		if tl, err = NewTrackingListener(ls); err != nil {
			logger.Fatal(kv{"fn": "newserver", "err": "Could not create listener: " + err.Error()})
			if s.Port == "0" {
				continue
			}
		}
		listener = tl
		if s.Port == "0" {
			s.Port = port
			s.Host += ":" + port
		}
		break
	}

	if s.IsHTTPS() {
		logger.Log(kv{"fn": "newserver", "msg": "Using https"})
		if listener, err = wrapHttps(tl, s.Cert, s.Key); err != nil {
			logger.Fatal(kv{"fn": "newserver", "err": "Could not create https listener: " + err.Error()})
		}
	}

	metaStore, err := NewMetaStore(s.MetaDB)
	if err != nil {
		logger.Fatal(kv{"fn": "main", "err": "Could not open the meta store: " + err.Error()})
	}

	contentStore, err := NewContentStore(s.ContentPath)
	if err != nil {
		logger.Fatal(kv{"fn": "main", "path": s.ContentPath, "err": "Could not open the content store: " + err.Error()})
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)
	go func(c chan os.Signal, listener net.Listener) {
		for {
			sig := <-c
			switch sig {
			case syscall.SIGHUP: // Graceful shutdown
				tl.Close()
			}
		}
	}(c, tl)

	logger.Log(kv{"fn": "newserver", "msg": "listening", "pid": os.Getpid(), "addr": ls, "version": version})
	if wc, err := os.Create(cfn); err == nil {
		defer wc.Close()
		w := bufio.NewWriter(wc)
		for i := 0; i < te.NumField(); i++ {
			sf := te.Field(i)
			name := sf.Name
			field := ve.FieldByName(name)
			envVar := strings.ToUpper(fmt.Sprintf("%s_%s", keyPrefix, name))
			if _, err := w.WriteString(envVar + "=" + field.String() + "\n"); err != nil {
				logger.Log(kv{"fn": "newserver", "var": envVar, "value": field.String(), "error": err})
			} else {
				logger.Log(kv{"fn": "newserver", "var": envVar, "value": field.String(), "ok": 1})
			}
		}
		w.Flush()
	}

	app := NewApp(contentStore, metaStore, server)
	s.Up = "up"

	if s.IsUsingTus() {
		tusServer.Start(server)
	}
	app.Serve(listener)
	tl.WaitForChildren()
	if s.IsUsingTus() {
		tusServer.Stop()
	}

	return s
}
