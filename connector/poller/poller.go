package poller

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// Poller wraps an OS-level epoll/kqueue instance.
// It registers client socket fds with the kernel and calls epoll_wait/kevent
// to block until the kernel signals that data is available on any registered fd.
type Poller struct {
	// or epoll Id
	kqueueId int
}

func NewPoller() (*Poller, error) {
	// Here we create epoll/kqueue object
	fd, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}

	return &Poller{kqueueId: fd}, nil
}

func (p *Poller) Run() {
	inboundEvents := make([]unix.Kevent_t, 100)
	for {
		// epoll_wait/kevent - it's a syscall to wait for incoming event
		// At this time epoll/kqueue OS object is asleep.
		// It will wake up when already registered client socket will send data for reading
		n, err := unix.Kevent(p.kqueueId, nil, inboundEvents, nil)

		if err != nil {
			fmt.Println("ERROR: ", err)
			continue
		}

		for i := 0; i < n; i++ {
			fmt.Printf("Client socket %d is ready for read\n", inboundEvents[i].Ident)
			//TODO: send to worker pool
		}
	}
}

func (p *Poller) Register(conn *net.TCPConn) error {
	file, err := conn.File()
	if err != nil {
		return err
	}

	defer file.Close()

	// We get here file descriptor id of the client socket
	fd := file.Fd()

	events := []unix.Kevent_t{
		{
			Ident: uint64(fd),
			// It means that by which event OS should notify an epoll object
			Filter: unix.EVFILT_READ,
			// It means that begin to check this client socket's fd
			Flags: unix.EV_ADD,
		},
	}

	// kevent/epoll_ctl - we register client socket fd in epoll/kqueue object (it doesn't mean that we create again client socket)
	_, err = unix.Kevent(p.kqueueId, events, nil, nil)

	return err
}
