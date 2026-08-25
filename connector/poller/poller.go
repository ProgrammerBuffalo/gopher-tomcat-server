package poller

import (
	"errors"
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

	p := &Poller{kqueueId: fd}
	go p.Run()

	return p, nil
}

func (p *Poller) Close() error {
	if err := unix.Close(p.kqueueId); err != nil {
		return err
	}
	fmt.Printf("Poller closed with fd: %d\n", p.kqueueId)
	return nil
}

func (p *Poller) Run() {
	inboundEvents := make([]unix.Kevent_t, 100)
	for {
		// epoll_wait/kevent - it's a syscall to wait for an incoming event
		// At this time an epoll/kqueue OS object is asleep.
		// It will wake up when an already registered client socket sends data for reading
		n, err := unix.Kevent(p.kqueueId, nil, inboundEvents, nil)

		if err != nil {
			// if the file descriptor of a kqueue/epoll object was closed (it will be closed by poller.Close())
			if errors.Is(err, unix.EBADF) {
				fmt.Println("Poller stopped (kqueue closed)")
				return
			}
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
	// Here we get the original client connection
	sysConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	var kEventErr error
	// func Control() uses runtime.KeepAlive() inside, which ensures the client socket will not be closed due to liveness analysis.
	// If we didn't use Control(), the GC (if a GC cycle triggered after sysConn initialization) would see that after sysConn initialization, the connection object is not used in the code below.
	// If the net.TCPConn is also not used in outer functions, the GC will collect it, close the client socket via its finalizer, and we will lose the connection.
	err = sysConn.Control(func(fd uintptr) {
		fmt.Printf("Registering client socket with ID: %d\n", int(fd))
		events := []unix.Kevent_t{
			{
				Ident: uint64(fd),
				// It means that by which event OS should notify an epoll object
				Filter: unix.EVFILT_READ,
				// It means that begin to check this client socket's fd
				Flags: unix.EV_ADD | unix.EV_CLEAR,
			},
		}
		// kevent/epoll_ctl - we register client socket fd in an epoll/kqueue object (it doesn't mean that we create again client socket)
		_, kEventErr = unix.Kevent(p.kqueueId, events, nil, nil)
	})

	if err != nil {
		return err
	}

	return kEventErr
}
