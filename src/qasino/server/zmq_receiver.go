package server

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/golang/glog"
	"github.com/pebbe/zmq4"
)

type ZmqReceiver struct {
	socket *zmq4.Socket
}

func NewZmqReceiver(port int) (*ZmqReceiver, error) {

	var err error

	z := &ZmqReceiver{}

	z.socket, err = zmq4.NewSocket(zmq4.REP)
	if err != nil {
		return nil, err
	}

	err = z.socket.Bind(fmt.Sprintf("tcp://*:%d", port))
	if err != nil {
		return nil, err
	}

	return z, nil
}

func (z *ZmqReceiver) ErrorResponse(errmsg, identity string) {

	glog.Errorln(errmsg)

	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(ErrorResponse(errmsg, identity))

	z.socket.SendMessage(buf.String())
}

func (z *ZmqReceiver) Run(d *DataManager) {
	var err error
	var msg []string

	for {

		msg, err = z.socket.RecvMessage(0)
		if err != nil {
			glog.Errorln(fmt.Sprintf("zmq_receiver: Error from zmq.socket.RecvMessage: %s", err))
			continue
		}
		if len(msg) < 1 {
			z.ErrorResponse("Empty message received", d.self_identity)
			continue
		}

		// TODO this is serial single threaded processing...

		var op RequestOp
		var response_buf bytes.Buffer

		msgbuf := bytes.NewBufferString(msg[0])
		decoder := json.NewDecoder(msgbuf)
		if err = decoder.Decode(&op); err != nil {
			z.ErrorResponse(fmt.Sprintf("Failed to parse request as JSON: %s: `%s`", err, msg[0]), d.self_identity)
			continue
		}

		if err != nil {
			z.ErrorResponse(fmt.Sprintf("Invalid input format: %s", err), d.self_identity)
			continue
		}

		switch op.Op {
		case "query":

			if op.Sql == "" {
				z.ErrorResponse(fmt.Sprintf("Query Op is missing 'sql' param"), d.self_identity)
				continue
			}

			result, err := process_sql_statement(op.Sql, d)
			if err != nil {
				z.ErrorResponse(fmt.Sprintf("Error processing sql: %s: %s", err, op.Sql), d.self_identity)
				continue
			}

			json.NewEncoder(&response_buf).Encode(DataSuccessResponse(result, d.self_identity))

			z.socket.SendMessage(response_buf.String())

		case "add_table_data":

			op.Table.SetProperty("update", op.Update)
			op.Table.SetProperty("static", op.Static)
			op.Table.SetProperty("persist", op.Persist)
			op.Table.SetProperty("keycols", op.KeyCols)
			op.Table.SetProperty("identity", op.Identity)

			identity := op.Table.GetStringProperty("identity")

			if op.Table.GetBoolProperty("static") {
				d.sql_backend_writer_static.Use(func(writer SqlReaderWriter) {
					err = d.add_table_data(writer, &op.Table, identity)
				})
			} else {
				d.sql_backend_writer.Use(func(writer SqlReaderWriter) {
					err = d.add_table_data(writer, &op.Table, identity)
				})
			}

			if err != nil {
				z.ErrorResponse(fmt.Sprintf("Error from add_table_data: %s", err), d.self_identity)
				continue
			}

			json.NewEncoder(&response_buf).Encode(BasicSuccessResponse(d.self_identity))

			z.socket.SendMessage(response_buf.String())

		default:
			z.ErrorResponse(fmt.Sprintf("Unrecognized operation '%s'", op.Op), d.self_identity)

		} // switch op.Op

	} // for
}
