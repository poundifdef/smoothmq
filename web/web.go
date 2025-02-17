package web

import (
	"crypto/tls"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/poundifdef/smoothmq/config"
)

type Web struct {
	FiberApp *fiber.App
	Path     string
	Port     int
	TLS      config.TLSConfig
	Type     string
}

func (w *Web) Start() error {
	port := fmt.Sprintf(":%d", w.Port)

	if w.TLS.Cert != "" {
		cer, err := tls.LoadX509KeyPair(w.TLS.Cert, w.TLS.PrivateKey)
		if err != nil {
			panic(err)
		}

		tlsCfg := &tls.Config{Certificates: []tls.Certificate{cer}}

		listener, err := tls.Listen("tcp", port, tlsCfg)
		if err != nil {
			panic(err)
		}

		w.OutputPort()
		return w.FiberApp.Listener(listener)
	}

	w.OutputPort()
	return w.FiberApp.Listen(port)
}

func (w *Web) Stop() error {
	return w.FiberApp.Shutdown()
}

func (w *Web) OutputPort() {
	if w.Type != "" {
		scheme := "http"
		if w.TLS.Cert != "" {
			scheme = "https"
		}

		fmt.Printf("%18s: %s://localhost:%d%s\n", w.Type, scheme, w.Port, w.Path)
	}
}
