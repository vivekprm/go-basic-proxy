package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Backend struct {
	net.Conn // Embedded type but we can use it's method in backend.
	Reader   *bufio.Reader
	Writer   *bufio.Writer
}

var backendQueue chan *Backend
var requestBytes map[string]int64
var requestLock sync.Mutex

func init() {
	requestBytes = make(map[string]int64)
	backendQueue = make(chan *Backend, 10)
}

func updateStats(req *http.Request, resp *http.Response) int64 {
	requestLock.Lock()
	defer requestLock.Unlock()

	bytes := requestBytes[req.URL.Path] + resp.ContentLength
	requestBytes[req.URL.Path] = bytes
	return bytes
}

func getBackend() (*Backend, error) {
	select {
	case be := <-backendQueue:
		return be, nil
	case <-time.After(100 * time.Millisecond):
		be, err := net.Dial("tcp", ":8081")
		if err != nil {
			return nil, err
		}
		return &Backend{
			Conn:   be,
			Reader: bufio.NewReader(be),
			Writer: bufio.NewWriter(be),
		}, nil
	}
}

func queueBackend(be *Backend) {
	select {
	case backendQueue <- be:
		// backend re-enqueued safely, move on.
	case <-time.After(1 * time.Second):
		be.Close()
	}
}

func main() {
	// 1. Listen for connections forever
	if ln, err := net.Listen("tcp", ":8787"); err == nil {
		for {
			// 2. Accept connections
			if conn, err := ln.Accept(); err == nil {
				go handleConnection(conn)
			}
		}
	}

}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		req, err := http.ReadRequest(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("Failed to read request: %s", err)
			}
			return
		}
		be, err := getBackend()
		if err != nil {
			return
		}
		if err := req.Write(be.Writer); err == nil {
			be.Writer.Flush()
			if resp, err := http.ReadResponse(be.Reader, req); err == nil {
				bytes := updateStats(req, resp)
				resp.Header.Set("X-Bytes", strconv.FormatInt(bytes, 10))
				if err := resp.Write(conn); err == nil {
					log.Printf("proxied %s: got %d", req.URL.Path, resp.StatusCode)
				}
			}
		}
		// We don't want to block the main thread while queuing
		go queueBackend(be)
	}
}
