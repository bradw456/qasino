package server

import (
	"encoding/json"
	"testing"
	"time"

	qasinotable "qasino/table"

	"github.com/pebbe/zmq4"
)

var zmq_server_running bool

func NewTestZmqServer(data_manager *DataManager) {
	if zmq_server_running {
		return
	}

	zmq_server_running = true

	zmq_receiver, err := NewZmqReceiver(15598)

	if err != nil {
		panic(err)
	}

	go zmq_receiver.Run(data_manager)

	time.Sleep(1 * time.Second)
}

func ZmqPublishTable(table *qasinotable.Table, t *testing.T) {
	op := RequestOp{
		Identity: "test",
		Op:       "add_table_data",
		Table:    *table,
	}

	json_string, err := json.Marshal(op)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	socket, err := zmq4.NewSocket(zmq4.REQ)
	if err != nil {
		FailFatal(t, "%s", err)

	}

	addr := "tcp://127.0.0.1:15598"
	err = socket.Connect(addr)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	// Send request
	_, err = socket.SendMessage(json_string)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	// Get response
	var msg []string
	msg, err = socket.RecvMessage(0)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	var resp_op ResponseOp

	err = json.Unmarshal([]byte(msg[0]), &resp_op)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	err = socket.Close()
	if err != nil {
		FailFatal(t, "%s", err)
	}

	if resp_op.ResponseOp != "ok" {
		FailFatal(t, "Response Op is not ok: %s", string(msg[0]))
	}

	if resp_op.Identity != "testident" {
		FailFatal(t, "Response Ident is not testident: %s", string(msg[0]))
	}
}

func ZmqQueryTable(sql string, t *testing.T) *SelectResultData {

	var err error

	op := RequestOp{
		Identity: "test",
		Op:       "query",
		Sql:      sql,
	}

	json_string, err := json.Marshal(op)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	socket, err := zmq4.NewSocket(zmq4.REQ)
	if err != nil {
		FailFatal(t, "%s", err)

	}

	addr := "tcp://127.0.0.1:15598"
	err = socket.Connect(addr)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	// Send request
	_, err = socket.SendMessage(json_string)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	// Get response
	var msg []string
	msg, err = socket.RecvMessage(0)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	var resp_op ResponseOp

	err = json.Unmarshal([]byte(msg[0]), &resp_op)
	if err != nil {
		FailFatal(t, "%s", err)
	}

	err = socket.Close()
	if err != nil {
		FailFatal(t, "%s", err)
	}

	if resp_op.ResponseOp != "result_table" {
		FailFatal(t, "Response Op is not result_table: %s", string(msg[0]))
	}

	if resp_op.Identity != "testident" {
		FailFatal(t, "Response Ident is not testident: %s", string(msg[0]))
	}

	if resp_op.Table == nil {
		FailFatal(t, "No table in response: %s", string(msg[0]))
	}

	return resp_op.Table
}

func TestZmqReceiver(t *testing.T) {

	d := GetTestDataManager(t)

	NewTestZmqServer(d)

	table := GetDummyTable()

	ZmqPublishTable(table, t)

	time.Sleep(2 * time.Second)
	d.RotateDbs()

	// Now verify the result with a query.

	sql := "SELECT * FROM dummy_test_table ORDER BY 1"

	result_table := ZmqQueryTable(sql, t)

	if CompareSelectResultAndTable(result_table, table) != 0 {
		FailFatal(t, "HttpQueryTable results from query are not expected: query ===>\n%s <=== != table ===>\n%s <===\n",
			SelectResultAsString(result_table), table.String())
	}

}
