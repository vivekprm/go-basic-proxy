package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
)

var requestBytes map[string]int64
var requestLock sync.Mutex

func init() {
	requestBytes = make(map[string]int64)
}

func updateStats(req *http.Request, resp *http.Response) int64 {
	requestLock.Lock()
	defer requestLock.Unlock()

	bytes := requestBytes[req.URL.Path] + resp.ContentLength
	requestBytes[req.URL.Path] = bytes
	return bytes
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
		if be, err := net.Dial("tcp", "127.0.0.1:8081"); err == nil {
			be_reader := bufio.NewReader(be)
			// 5. Send the request to the backend.
			if err := req.Write(be); err == nil {
				if resp, err := http.ReadResponse(be_reader, req); err == nil {
					bytes := updateStats(req, resp)
					resp.Header.Set("X-Bytes", strconv.FormatInt(bytes, 10))
					if err := resp.Write(conn); err == nil {
						log.Printf("proxied %s: got %d", req.URL.Path, resp.StatusCode)
					}
				}
			}
		}
	}
}
