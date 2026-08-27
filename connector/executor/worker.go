package executor

import (
	"bytes"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// This separator is used to understand the end of headers of http
var headerSeparator = []byte("\r\n\r\n")

// Worker is a goroutine that reads-writes data for client sockets
type Worker struct {
	id         int
	socketIdCh chan int

	pool *WorkerPool
}

func NewWorker(id int, pool *WorkerPool) *Worker {
	w := &Worker{socketIdCh: make(chan int), id: id, pool: pool}
	go w.Run()

	return w
}

// Run will use NIO - Non-Blocking I/O style.
// It means that if we couldn't fully read data the first time, worker will not wait until the 2nd package of http headers comes.
func (w *Worker) Run() {
	// Because of EV_CLEAR in the poller, maybe we couldn't read income data one time that signaled OS
	buffer := make([]byte, 1024)
	endIndex := -1
	// Here a worker is waiting for a new socket id
	for socketId := range w.socketIdCh {
		fmt.Printf("Worker %d got data\n", w.id)
		var existingBuffer []byte

		// Extract socket's data that we have already read in previous cycles
		if clientData, ok := w.pool.activeSockets.Load(socketId); ok {
			existingBuffer = clientData.([]byte)
		}

		for {
			n, err := unix.Read(socketId, buffer)
			if err != nil {
				// it means that we have read the latest bytes from the socket's buffer
				if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
					w.pool.activeSockets.Store(socketId, existingBuffer)
					w.pool.rearmFunc(socketId)
				} else {
					fmt.Printf("Client socket disconnection %d:%v\n", socketId, err)
					w.pool.activeSockets.Delete(socketId)
					err = unix.Close(socketId)
					if err != nil {
						fmt.Println("ERROR while closing client socket: ", err)
					}
				}
				w.pool.workers <- w
				break
			}
			if n == 0 {
				fmt.Printf("Client %d disconnected, no bytes read\n", socketId)
				w.pool.activeSockets.Delete(socketId)
				err = unix.Close(socketId)
				w.pool.workers <- w
				break
			}
			// Here we add to existing socket's bytes that have been read at previous cycles (not only here, can be by another worker)
			existingBuffer = append(existingBuffer, buffer[:n]...)

			// We check, was it the latest bytes of http headers
			endIndex = bytes.Index(existingBuffer, headerSeparator)
			if endIndex != -1 {
				// re-enable the opportunity of reading from socket for poller
				w.pool.rearmFunc(socketId)
				// notify to worker pool that the worker is ready for a new socket reading iteration
				w.pool.workers <- w
				break
			} else {
				w.handleRequest(existingBuffer)
			}
		}

	}
}

func (w *Worker) handleRequest(existingBuffer []byte) {
	fmt.Printf("Worker %d successfully read data: %s\n", w.id, string(existingBuffer))
}
