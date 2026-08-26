package poller

import (
	"errors"
	"fmt"
	"gopher-tomcat-server/connector/executor"
	"net"

	"golang.org/x/sys/unix"
)

// Poller wraps an OS-level epoll/kqueue instance.
// It registers client socket fds with the kernel and calls epoll_wait/kevent
// to block until the kernel signals that data is available on any registered fd.
type Poller struct {
	// or epoll Id
	kqueueId int

	workerPool *executor.WorkerPool
}

func NewPoller() (*Poller, error) {
	// Here we create epoll/kqueue object
	fd, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}

	p := &Poller{kqueueId: fd}

	wp := executor.NewWorkerPool(3, p.Rearm)
	p.workerPool = wp

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

			// By flags and & with EV_EOF we can check do we have in the event's flag the bit of client disconnection == EV_EOF
			// When a client disconnects it sends special TCP package - FIN (Finish).
			// By this OS marks for our epoll/kqueue object that in this client socket (fd) occurred a disconnection event

			// EV_ERROR - can be occurred when the client's program was unexpectedly closed
			// When in a client's program occurred a critical error, the client's OS sends a special TCP package-RST (Reset)
			if (inboundEvents[i].Flags & (unix.EV_EOF | unix.EV_ERROR)) != 0 {
				fmt.Printf("Client socket id: %d is disconnected", inboundEvents[i].Ident)

				// We need to send syscall to OS for close release resources of socket from OS
				// If we don't call this, the socket will remain in the CLOSE_WAIT state indefinitely, causing a resource leak until the app shuts down.
				err = unix.Close(int(inboundEvents[i].Ident))
				if err != nil {
					fmt.Println("Closing client socket error: ", err)
				}

				continue
			}

			p.workerPool.AddSocketTask(int(inboundEvents[i].Ident))
		}
	}
}

// Rearm Once our worker is done reading, it manually re-enables (re-arms) the event in the OS
func (p *Poller) Rearm(socketId int) {
	fmt.Printf("Rearm client socket reading with ID: %d\n", socketId)
	events := []unix.Kevent_t{{
		Ident:  uint64(socketId),
		Filter: unix.EVFILT_READ,
		Flags:  unix.EV_ENABLE | unix.EV_CLEAR | unix.EV_ONESHOT,
	}}
	// re-enable the client socket for the next reading
	_, err := unix.Kevent(p.kqueueId, events, nil, nil)
	if err != nil {
		fmt.Println("ERROR while rearm", err)
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
				// EV_CLEAR - means that OS will say one time about that new data has come to the socket, it will not spam
				// EV_ONESHOT - is used to avoid race conditions
				//              Once an event is triggered, the OS automatically suppresses any later notifications for that file descriptor.
				//              Even if new data arrives, the poller won't wake up another thread.
				Flags: unix.EV_ADD | unix.EV_CLEAR | unix.EV_ONESHOT,
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
