package connector

import (
	"fmt"
	"gopher-tomcat-server/connector/acceptor"
	"gopher-tomcat-server/connector/poller"
	"net"
	"sync"
)

// Connector is used for open server socket
type Connector struct {
	listener net.Listener

	acr acceptor.Acceptor

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
		fmt.Println("ERROR while creating listener: ", err)
		return err
	}

	p, err := poller.NewPoller()

	if err != nil {
		fmt.Println("ERROR while creating poller: ", err)
		err = listener.Close()
		if err != nil {
			return err
		}
		return err
	}

	c.wg = sync.WaitGroup{}
	c.listener = listener

	c.acr = acceptor.NewAcceptor(listener, p, &c.wg)

	go c.acr.Run()

	fmt.Println("Connector started...")
	return nil
}

func (c *Connector) Stop() error {
	if err := c.acr.Close(); err != nil {
		fmt.Println("ERROR while closing acceptor: ", err)
		return err
	}
	fmt.Println("Connector closed...")
	return nil
}
