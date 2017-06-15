package server

import (
	"fmt"

	"github.com/pebbe/zmq4"
)

type SignalChannel struct {
	context *zmq4.Context
	socket  *zmq4.Socket
}

func NewSignalChannel() *SignalChannel {
	return &SignalChannel{}
}

func (s *SignalChannel) Open(port int) error {
	var err error

	s.context, err = zmq4.NewContext()
	if err != nil {
		return err
	}

	s.socket, err = s.context.NewSocket(zmq4.PUB)
	if err != nil {
		return err
	}

	s.socket.Bind(fmt.Sprintf("tcp://*:%d", port))

	return nil
}

func (s *SignalChannel) Close() {
	if s.context != nil {
		s.context.Term()
	}
	if s.socket != nil {
		s.socket.Close()
	}
}

func (s *SignalChannel) Send(generation_number int64, generation_interval_s int, identity string) {

	// tag + b'\0' + message
	msg := fmt.Sprintf(`GENSIG%s{"op":"generation_signal","identity":"%s","generation_number":%d,"generation_duration_s":%d}`,
		"\x00", identity, generation_number, generation_interval_s)

	//  Send message to all subscribers

	s.socket.Send(msg, 0)
}
