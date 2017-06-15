package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/golang/glog"
)

func LineReceiverAccept(port int, data_manager *DataManager) {

	var connection_id int = 1

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		// handle error
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			// handle error
		}
		go NewLineReceiver(data_manager, connection_id).Receive(conn)
		connection_id++
	}
}

type LineReceiver struct {
	data_manager  *DataManager
	connection_id int
	sql_statement bytes.Buffer
}

func NewLineReceiver(data_manager *DataManager, connection_id int) *LineReceiver {
	return &LineReceiver{
		data_manager:  data_manager,
		connection_id: connection_id,
	}
}

func (l *LineReceiver) Receive(conn net.Conn) {
	conn.SetDeadline(time.Now().Add(time.Minute * 1))

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Printf("Line: %s\n", line)

		bye_time := l.lineReceived(conn, line)

		if bye_time {
			break
		}

	}
	if err := scanner.Err(); err != nil {
		glog.Errorf("Error reading from connection: %s\n", err)

	}
	conn.Close()
}

var exit_regexp *regexp.Regexp = regexp.MustCompile(`^\s*[\x03\x04]+\s*$`)
var semi_colon_ended_regexp *regexp.Regexp = regexp.MustCompile(`;\s*$`)

func (l *LineReceiver) lineReceived(conn net.Conn, line string) bool {

	if exit_regexp.MatchString(line) {
		io.Copy(conn, bytes.NewReader([]byte("Bye!\n")))
		// Return true to end the session.
		return true
	}

	l.sql_statement.WriteString(line)
	l.sql_statement.WriteString("\n")

	if semi_colon_ended_regexp.MatchString(line) {

		fmt.Printf("found semi\n")

		// Send the SQL statement and reply with a response.

		sql_statement := l.sql_statement.String()

		var response string

		result, err := l.process_sql_statement(sql_statement)
		if err != nil {
			errmsg := fmt.Sprintf("Error processing sql: %s: %s", err, sql_statement)
			glog.Errorln(errmsg)
			response = fmt.Sprintf("Error in executing SQL statement: %s\n", err)
		} else {
			response = result.String()
		}

		_, err = io.Copy(conn, bytes.NewReader([]byte(response)))
		if err != nil {
			glog.Errorf("Error copying results to remote, closing connection: %s\n", err)
			// Return true to end the session.
			return true
		}

		l.sql_statement.Reset()
	}

	fmt.Printf("no semi: so far: %s\n", l.sql_statement.String())

	// Return false to continue getting statements from the client.
	return false
}

func (l *LineReceiver) process_sql_statement(sql_statement string) (*SelectResult, error) {
	query_id := l.data_manager.get_query_id()

	query_start := time.Now()

	glog.Infof("line_receiver: (%d:%d) SQL received: %s", l.connection_id, query_id, sql_statement)

	var err error = nil
	var result *SelectResult

	l.data_manager.sql_backend_reader.Use(func(reader SqlReaderWriter) {

		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(sql_statement)), "select") {

			// Select statement
			result, err = l.data_manager.do_select(reader, query_id, sql_statement)

		} else {

			result, err = l.data_manager.process_non_select(reader, query_id, sql_statement)
		}
	})

	resultstr := "success"
	if err != nil {
		resultstr = "failure"
	}

	glog.Infof("line_receiver: (%d:%d) SQL completed (%s) result is %s\n",
		l.connection_id, query_id, time.Since(query_start), resultstr)

	return result, err
}
