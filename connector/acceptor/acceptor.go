package acceptor

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

// Acceptor is used for get client sockets from OS
type Acceptor struct {
	listener net.Listener

	wg *sync.WaitGroup
}

func Initialize(listener net.Listener, wg *sync.WaitGroup) Acceptor {
	return Acceptor{listener: listener, wg: wg}
}

func (a *Acceptor) Run() {
	a.wg.Add(1)
	defer a.wg.Done()
	for {
		fmt.Println("Acceptor waiting for new client connection...")

		// If no connection is ready, the goroutine is parked (_GWaiting) while waiting for I/O.
		// The Go netpoller waits for events using epoll_wait() on Linux.
		// epoll_wait() blocks in the kernel until an event occurs on a registered fd (server file descriptor)
		// When the listening socket (server socket) becomes ready (means that for server socket we have ready client socket),
		// epoll_wait() returns the event.
		// The Go runtime then marks the waiting goroutine as runnable (_GRunnable).
		// The Go scheduler will eventually execute the goroutine.
		_, err := a.listener.Accept()

		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				fmt.Println("Acceptor is closed")
				return
			}
			fmt.Println("ERROR: ", err)
			continue
		}

		fmt.Println("Acceptor got new client socket")
	}
}

func (a *Acceptor) Close() error {
	if err := a.listener.Close(); err != nil {
		return err
	}
	fmt.Println("Acceptor closed...")
	return nil
}
