package connector

import (
	"fmt"
	"gopher-tomcat-server/connector/acceptor"
	"net"
	"sync"
)

// Connector is used for open server socket
type Connector struct {
	listener net.Listener
	acceptor acceptor.Acceptor

	wg sync.WaitGroup
}

// Start opens the server's socket at a specified port via TCP protocol
func (c *Connector) Start() error {
	// net.Listen does:
	// 1) socket() -> creates a socket and gets a file descriptor
	// 2) bind() -> after that binds this file descriptor to port
	// 3) listen() -> opens socket for new tcp connections

	// When a client completes the TCP handshake, the kernel places the
	// established connection into the listening socket's accept queue.
	// The listening socket remains the same; accept() later removes one
	// pending connection from this queue and returns a new socket for it.

	// Here we create a kernel object as **socket object**
	listener, err := net.Listen("tcp", ":8080")

	if err != nil {
		return err
	}
	c.wg = sync.WaitGroup{}

	c.listener = listener
	c.acceptor = acceptor.Initialize(listener, &c.wg)

	go c.acceptor.Run()

	fmt.Println("Connector started...")
	return nil
}

func (c *Connector) Stop() error {
	if err := c.acceptor.Close(); err != nil {
		fmt.Println("ERROR while closing acceptor: ", err)
		return err
	}
	fmt.Println("Connector closed...")
	return nil
}
