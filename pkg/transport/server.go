package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
)

// Server is a TCP server that accepts connections and dispatches messages
// by msgType to registered handlers.
type Server struct {
	listener   net.Listener
	handlers   map[uint8]MessageHandler
	rpcHandler RPCHandler
	mu         sync.RWMutex
	accepted   map[net.Conn]struct{}
	acceptedMu sync.Mutex
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// NewServer creates a new Server. Register handlers before calling Start.
func NewServer() *Server {
	return &Server{
		handlers: make(map[uint8]MessageHandler),
		accepted: make(map[net.Conn]struct{}),
		stopCh:   make(chan struct{}),
	}
}

// Handle registers a handler for a message type.
// Panics if msgType is 0, 0xFE, or 0xFF (reserved).
func (s *Server) Handle(msgType uint8, h MessageHandler) {
	if msgType == 0 || msgType == MsgTypeRPCRequest || msgType == MsgTypeRPCResponse {
		panic("nodetransport: reserved message type")
	}
	s.mu.Lock()
	s.handlers[msgType] = h
	s.mu.Unlock()
}

// HandleRPC registers the RPC request handler for 0xFE messages.
func (s *Server) HandleRPC(h RPCHandler) {
	s.mu.Lock()
	s.rpcHandler = h
	s.mu.Unlock()
}

func (s *Server) HandleRPCMux(mux *RPCMux) {
	if mux == nil {
		panic("nodetransport: nil rpc mux")
	}
	s.HandleRPC(mux.HandleRPC)
}

// Start begins listening on addr.
func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = ln
	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

// Stop closes the listener and all accepted connections, then waits for
// all goroutines to exit.
func (s *Server) Stop() {
	close(s.stopCh)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.acceptedMu.Lock()
	for c := range s.accepted {
		_ = c.Close()
	}
	s.acceptedMu.Unlock()
	s.wg.Wait()
}

// Listener returns the underlying net.Listener.
func (s *Server) Listener() net.Listener {
	return s.listener
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				continue
			}
		}
		setTCPKeepAlive(conn)
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()

	s.acceptedMu.Lock()
	s.accepted[conn] = struct{}{}
	s.acceptedMu.Unlock()

	defer func() {
		conn.Close()
		s.acceptedMu.Lock()
		delete(s.accepted, conn)
		s.acceptedMu.Unlock()
	}()

	var writeMu sync.Mutex

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		msgType, body, err := ReadMessage(conn)
		if err != nil {
			return
		}

		switch msgType {
		case MsgTypeRPCRequest:
			s.mu.RLock()
			h := s.rpcHandler
			s.mu.RUnlock()
			if h != nil {
				s.wg.Add(1)
				go s.handleRPCRequest(conn, &writeMu, h, body)
			}
		case MsgTypeRPCResponse:
			// Responses should not arrive on server-accepted connections
		default:
			s.mu.RLock()
			h := s.handlers[msgType]
			s.mu.RUnlock()
			if h != nil {
				h(conn, body)
			}
		}
	}
}

func (s *Server) handleRPCRequest(conn net.Conn, writeMu *sync.Mutex, handler RPCHandler, body []byte) {
	defer s.wg.Done()

	if len(body) < 8 {
		return
	}
	requestID := binary.BigEndian.Uint64(body[0:8])
	payload := body[8:]

	ctx := context.Background()
	respData, err := handler(ctx, payload)
	var errCode uint8
	if err != nil {
		errCode = 1
		respData = []byte(err.Error())
	}

	respBody := encodeRPCResponse(requestID, errCode, respData)
	writeMu.Lock()
	_ = WriteMessage(conn, MsgTypeRPCResponse, respBody)
	writeMu.Unlock()
}

// encodeRPCRequest encodes [requestID:8][payload:N].
func encodeRPCRequest(requestID uint64, payload []byte) []byte {
	buf := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint64(buf[0:8], requestID)
	copy(buf[8:], payload)
	return buf
}

// encodeRPCResponse encodes [requestID:8][errCode:1][data:N].
func encodeRPCResponse(requestID uint64, errCode uint8, data []byte) []byte {
	buf := make([]byte, 9+len(data))
	binary.BigEndian.PutUint64(buf[0:8], requestID)
	buf[8] = errCode
	copy(buf[9:], data)
	return buf
}

// decodeRPCResponse decodes [requestID:8][errCode:1][data:N].
func decodeRPCResponse(body []byte) (requestID uint64, errCode uint8, data []byte, err error) {
	if len(body) < 9 {
		return 0, 0, nil, fmt.Errorf("nodetransport: rpc response body too short: %d", len(body))
	}
	requestID = binary.BigEndian.Uint64(body[0:8])
	errCode = body[8]
	data = body[9:]
	return requestID, errCode, data, nil
}
