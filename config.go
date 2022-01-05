package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
)

// Configuration holds application configuration. Values will be pulled from
// environment variables, prefixed by keyPrefix. Default values can be added
// via tags.
type Configuration struct {
	Listen      string `config:"tcp://:8080"`
	Host        string `config:"localhost:8080"`
	MetaDB      string `config:"lfs.db"`
	Size        string `config:"2000000000"`
	ContentPath string `config:"lfs-content"`
	Server      string `config:"root"`
	AdminPass   string `config:"admin"`
	ReaderPass  string `config:"reader"`
	Cert        string `config:""`
	Key         string `config:""`
	Scheme      string `config:"http"`
	Public      string `config:"public"`
	UseTus      string `config:"false"`
	TusHost     string `config:"localhost:1080"`
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

// Config is the global app configuration
var Config map[string]*Configuration = map[string]*Configuration{}

const keyPrefix = "LFS"

func newserver(server string) *Configuration {
	s, ok := Config[server]
	if ok {
		return s
	}
	s = &Configuration{}

	var local map[string]string = map[string]string{}

	local["LFS_SERVER"] = server
	if file, err := os.Open(server + "/.config"); err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			arr := strings.SplitN(scanner.Text(), "=", 1)
			if len(arr) == 2 {
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
		}
		field.SetString(env)
	}

	Config[server] = s

	var listener net.Listener

	tl, err := NewTrackingListener(s.Listen)
	if err != nil {
		logger.Fatal(kv{"fn": "newserver", "err": "Could not create listener: " + err.Error()})
	}

	listener = tl

	if s.IsHTTPS() {
		logger.Log(kv{"fn": "newserver", "msg": "Using https"})
		listener, err = wrapHttps(tl, s.Cert, s.Key)
		if err != nil {
			logger.Fatal(kv{"fn": "newserver", "err": "Could not create https listener: " + err.Error()})
		}
	}

	metaStore, err := NewMetaStore(s.MetaDB)
	if err != nil {
		logger.Fatal(kv{"fn": "main", "err": "Could not open the meta store: " + err.Error()})
	}

	contentStore, err := NewContentStore(s.ContentPath)
	if err != nil {
		logger.Fatal(kv{"fn": "main", "err": "Could not open the content store: " + err.Error()})
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

	logger.Log(kv{"fn": "newserver", "msg": "listening", "pid": os.Getpid(), "addr": s.Listen, "version": version})

	app := NewApp(contentStore, metaStore, server)
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
