package executor

import (
	"fmt"
	"sync"
)

type WorkerPool struct {
	readySocketQueue chan int
	workers          chan *Worker

	// Since TCP is a stream protocol, HTTP headers may be fragmented across multiple TCP packets
	// activeSockets holds partially or fully read data for each client.
	// Workers DO NOT block waiting for more data. If a request is incomplete,
	// a worker saves the bytes here and returns to the pool. When the OS notifies
	// the Poller about new data (for our case new headers), a worker will retrieve the state from activeSockets and continue.
	// Key (int): client socket's file descriptor (fd)
	// Value ([]byte): accumulated incoming data (http headers until headerSeparator)
	activeSockets sync.Map

	// rearmFunc is a function used to re-register a socket with the OS-level poller for monitoring, based on its socket ID.
	rearmFunc func(socketId int)
}

func NewWorkerPool(poolSize int, rearmFunc func(int)) *WorkerPool {
	wp := WorkerPool{
		readySocketQueue: make(chan int, 200),
		activeSockets:    sync.Map{},
		workers:          make(chan *Worker, poolSize),
		rearmFunc:        rearmFunc}

	go wp.Run(poolSize)
	return &wp
}

func (wp *WorkerPool) AddSocketTask(socketId int) {
	fmt.Printf("Registered task for socket %d\n", socketId)
	wp.readySocketQueue <- socketId
}

func (wp *WorkerPool) Run(poolSize int) {
	// Register workers
	for poolSize > 0 {
		w := NewWorker(poolSize, wp)
		wp.workers <- w
		fmt.Printf("Registered worker %d\n", poolSize)
		poolSize--
	}
	for socketId := range wp.readySocketQueue {
		wp.activeSockets.LoadOrStore(socketId, make([]byte, 0, 8096))
		// get available worker
		w := <-wp.workers
		// notify available worker about a new socket
		w.socketIdCh <- socketId
	}
}
