package main

import (
	"fmt"
	"gopher-tomcat-server/connector"
	"os"
	"os/signal"
)

type Server struct {
	connector connector.Connector
}

func main() {
	s := Server{connector: connector.Connector{}}

	if err := s.connector.Start(); err != nil {
		fmt.Println("Server start error", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	<-signals

	if err := s.connector.Stop(); err != nil {
		fmt.Println(err)
	}
}
