//
// Go Qasino Client for sending SQL queries and related statements to
// a Qasino server (https://github.com/MediaMath/qasino).
//
// All queries are sent by single Http requests to Qasino's http or
// https endpoint with the option of basic auth if required.  This
// client is meant for interactive use not bulk querying.
//
// Readline is used for command line shell niceness including history
// and multiline support.  Much of the code using the readline library
// has been adapted from the cockroachdb cli.
//
// bwasson@mediamath.com Mar-2016
//
package main

import (
	"bytes"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/chzyer/readline"
	isatty "github.com/mattn/go-isatty"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/pflag"
)

var port *int = pflag.IntP("port", "p", 0, "Qasino port")
var hostname *string = pflag.StringP("hostname", "H", "", "Qasino hostname")
var username *string = pflag.StringP("username", "u", "", "Qasino username")
var password *string = pflag.StringP("password", "w", "", "Qasino password")
var output_format *string = pflag.StringP("output-format", "t", "text", "Format of output tables: text, json, csv, or html")
var query *string = pflag.StringP("query", "q", "", "Query to run, then exit")
var timeout_s *int = pflag.IntP("timeout", "T", 5, "Timeout in seconds")
var help *bool = pflag.BoolP("help", "h", false, "Show help")

var version = "1.0"
var identity string

func cmdLineHelp() {
	pflag.Usage()
	fmt.Printf(`

Set the following environment variables to avoid needing to pass
corresponding command line parameters:

    QASINO_CLIENT_HOSTNAME
    QASINO_CLIENT_USERNAME
    QASINO_CLIENT_PASSWORD

`)

}

func cmdHelp() {
	fmt.Printf(`
Supported statements:

    SELECT ... ;

    SHOW TABLES [ LIKE '<pattern>' ]

    SHOW VIEWS

    SHOW INFO

    SHOW CONNECTIONS

    DESC <tablename>

    DESC VIEW <viewname>

`)
}

const (
	cliNextLine     = 0 // Done with this line, ask for another line.
	cliExit         = 1 // User requested to exit.
	cliProcessQuery = 2 // Regular query to send to the server.
)

func main() {

	var last_result int = 0

	pflag.Parse()

	if *help {
		cmdLineHelp()
		os.Exit(1)
	}

	*hostname = get_first_set(*hostname, os.Getenv("QASINO_CLIENT_HOSTNAME"), "localhost")
	*username = get_first_set(*username, os.Getenv("QASINO_CLIENT_USERNAME"))
	*password = get_first_set(*password, os.Getenv("QASINO_CLIENT_PASSWORD"))

	identity = fmt.Sprintf("go-qasino-client;version=%s,user=%s", version, get_first_set(*username, os.Getenv("USER"), "none"))

	if *query != "" {
		outStr, err := runQuery(*query)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(outStr)
		os.Exit(0)
	}

	if *port != 0 {
		fmt.Fprintf(os.Stderr, "Connecting to %s:%s.\n", *hostname, *port)
	} else {
		fmt.Fprintf(os.Stderr, "Connecting to %s.\n", *hostname)
	}

	// Full disclosure: a bunch of this has come from cockroachdb's cli code.

	if isatty.IsTerminal(os.Stdin.Fd()) {
		// We only enable history management when the terminal is actually
		// interactive. This saves on memory when e.g. piping a large SQL
		// script through the command-line client.
		userAcct, err := user.Current()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot load or save the command-line history: %s\n", err)
		} else {
			histFile := filepath.Join(userAcct.HomeDir, ".qasino_client_history")
			readline.SetHistoryPath(histFile)
		}
	}

	var stmt []string

	fullPrompt := "qasino> "
	continuePrompt := "      > "

	for {
		thisPrompt := fullPrompt
		if len(stmt) > 0 {
			thisPrompt = continuePrompt
		}
		l, err := readline.Line(thisPrompt)

		if err == readline.ErrInterrupt || err == io.EOF {
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "Input error: %s\n", err)
			os.Exit(1)
		}

		tl := strings.TrimSpace(l)

		// Check if this is a request for help or a client-side command.
		// If so, process it directly and skip query processing below.
		status := handleInputLine(&stmt, tl)
		if status == cliNextLine {
			continue
		} else if status == cliExit {
			break
		}

		// We join the statements back together with newlines in case
		// there is a significant newline inside a string literal. However
		// we join with spaces for keeping history, because otherwise a
		// history recall will only pull one line from a multi-line
		// statement.
		fullStmt := strings.Join(stmt, "\n")

		// We save the history between each statement, This enables
		// reusing history in another SQL shell without closing the
		// current shell.
		//
		// AddHistory will push command into memory and try to persist
		// to disk (if readline.SetHistoryPath was called).
		// err can be not nil only if it got a IO error while
		// trying to persist.
		if err = readline.AddHistory(strings.Join(stmt, " ")); err != nil {
			fmt.Fprintf(os.Stderr, "Cannot save command-line history: %s\n", err)
			readline.SetHistoryPath("")
		}

		var outStr string

		outStr, err = runQuery(fullStmt)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			last_result = 1
		} else {
			fmt.Println(outStr)
			last_result = 0
		}

		// Clear the saved statement.
		stmt = stmt[:0]
	}

	os.Exit(last_result)
}

// handleInputLine looks at a single line of text entered
// by the user at the prompt and decides what to do: either
// run a client-side command, print some help or continue with
// a regular query.
func handleInputLine(stmt *[]string, line string) int {
	if len(*stmt) == 0 {
		// Special case: first line of potential multi-line statement.
		// In this case ignore empty lines, and recognize "help" specially.
		switch line {
		case "":
			return cliNextLine
		case "exit", "quit":
			return cliExit
		case "help":
			cmdHelp()
			return cliNextLine
		}
	}

	*stmt = append(*stmt, line)

	// Only SELECT statements are semi-colon terminated and support multi-line atm.
	if len(*stmt) > 0 || strings.HasPrefix(strings.ToLower(line), "select") {
		if !strings.HasSuffix(line, ";") {
			// Not yet finished with multi-line statement.
			return cliNextLine
		}
	}

	return cliProcessQuery
}

// Return the first non-"" value or "" if all are ""
func get_first_set(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func runQuery(sql string) (string, error) {

	lc_sql := strings.TrimSpace(strings.ToLower(sql))

	if !strings.HasPrefix(lc_sql, "show") && !strings.HasPrefix(lc_sql, "desc") && !strings.HasPrefix(lc_sql, "select") {
		return "Unrecognized statement.", nil
	}

	op := QasinoOp{
		Identity: identity,
		Op:       "query",
		Sql:      sql,
	}

	response, err := op.Query(*hostname, *username, *password, *port)

	if err != nil {
		errorstr := fmt.Sprintf("Error querying qasino %s: %s\n", *hostname, err)
		return errorstr, errors.New(errorstr)
	}

	switch *output_format {
	case "text":
		return response.Table.PrettyString(), nil
	case "json":
		return response.Table.JsonString(), nil
	case "html":
		return response.Table.HtmlString(), nil
	case "csv":
		return response.Table.CsvString(), nil
	default:
		return response.Table.PrettyString(), nil
	}
}

type QasinoTableData struct {
	Tablename   string          `json:"tablename,omitempty"`
	ColumnNames []string        `json:"column_names,omitempty"`
	ColumnTypes []string        `json:"column_types,omitempty"`
	Rows        [][]interface{} `json:"rows,omitempty"`
}

func (t *QasinoTableData) PrettyString() string {

	outbuf := &bytes.Buffer{}

	table := tablewriter.NewWriter(outbuf)
	table.SetHeader(t.ColumnNames)
	table.SetBorder(false)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	//table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.SetCenterSeparator(" ")
	table.SetColumnSeparator(" ")

	string_rows := make([][]string, len(t.Rows))

	for row_idx, row := range t.Rows {
		string_rows[row_idx] = make([]string, len(row))
		for cell_idx, cell := range row {
			string_rows[row_idx][cell_idx] = fmt.Sprintf("%v", cell)
		}
	}

	table.AppendBulk(string_rows)
	table.Render()

	outbuf.WriteString(fmt.Sprintf("\n%d rows returned\n", len(t.Rows)))

	return outbuf.String()
}

func (t *QasinoTableData) PrettyStringOld() string {
	var outbuf bytes.Buffer

	max_widths := make([]int, len(t.ColumnNames))

	string_rows := make([][]string, len(t.Rows))

	for idx, column_name := range t.ColumnNames {
		if max_widths[idx] < len(column_name) {
			max_widths[idx] = len(column_name)
		}
	}

	for row_idx, row := range t.Rows {

		string_rows[row_idx] = make([]string, len(row))
		for cell_idx, cell := range row {
			cell_str := fmt.Sprintf("%v", cell)

			/*
				switch cell.(type) {
				case int:
					cell_str = fmt.Sprintf("%d", cell)
				case string:
					cell_str = fmt.Sprintf("%s", cell)
				default:
					cell_str = fmt.Sprintf("%v", cell)
				}
			*/
			if max_widths[cell_idx] < len(cell_str) {
				max_widths[cell_idx] = len(cell_str)
			}
			string_rows[row_idx][cell_idx] = cell_str
		}
	}

	// Print the columns

	for idx, column_name := range t.ColumnNames {
		outbuf.WriteString(fmt.Sprintf("%*s  ", max_widths[idx], column_name))
	}
	outbuf.WriteString("\n")

	// Print the dividers

	for idx, _ := range t.ColumnNames {
		for i := 0; i < max_widths[idx]; i++ {
			outbuf.WriteString("=")
		}
		outbuf.WriteString("  ")
	}
	outbuf.WriteString("\n")

	// Now finally the data

	for _, row := range string_rows {
		for cell_idx, cell := range row {
			outbuf.WriteString(fmt.Sprintf("%*s  ", max_widths[cell_idx], cell))
		}
		outbuf.WriteString("\n")
	}

	outbuf.WriteString(fmt.Sprintf("%d rows returned", len(string_rows)))

	return outbuf.String()
}

func (t *QasinoTableData) JsonString() string {

	bytes, err := json.MarshalIndent(t, "", "    ")
	if err != nil {
		return fmt.Sprintf(`{
    "status" : "error", 
    "message" : "Error marshalling qasino table data: %s"
}`, err)
	}

	return string(bytes)
}

func (t *QasinoTableData) HtmlString() string {

	var outbuf bytes.Buffer

	outbuf.WriteString("<table>\n  <thead><tr>")

	for _, column_name := range t.ColumnNames {
		outbuf.WriteString(fmt.Sprintf("<th>%s</th>", column_name))
	}
	outbuf.WriteString("</tr></thead>\n  <tbody>\n")

	for _, row := range t.Rows {
		outbuf.WriteString("  <tr>")
		for _, cell := range row {
			outbuf.WriteString(fmt.Sprintf("<td>%v</td>", cell))
		}
		outbuf.WriteString("</tr>\n")
	}

	outbuf.WriteString("  </tbody>\n</table>")

	return outbuf.String()
}

func (t *QasinoTableData) CsvString() string {

	outbuf := &bytes.Buffer{}

	writer := csv.NewWriter(outbuf)

	writer.Write(t.ColumnNames)

	row_str := make([]string, len(t.ColumnNames))

	for _, row := range t.Rows {

		for i := 0; i < len(t.ColumnNames); i++ {
			if i >= len(row) {
				row_str[i] = ""
			} else {
				row_str[i] = fmt.Sprintf("%v", row[i])
			}
		}

		writer.Write(row_str)
	}
	writer.Flush()

	return outbuf.String()
}

type QasinoOp struct {
	Op       string           `json:"op,omitempty"`
	Persist  int              `json:"persist,omitempty"`
	Identity string           `json:"identity,omitempty"`
	Table    *QasinoTableData `json:"table,omitempty"`
	Sql      string           `json:"sql,omitempty"`
}

type QasinoOpResponse struct {
	ResponseOp string           `json:"response_op,omitempty"`
	Identity   string           `json:"identity,omitempty"`
	Message    string           `json:"error_message,omitempty"`
	MaxWidths  map[string]int   `json:"max_widths,omitempty"`
	Table      *QasinoTableData `json:"table,omitempty"`
}

func (o *QasinoOp) Query(hostname, username, password string, port int) (*QasinoOpResponse, error) {

	json_string, err := json.Marshal(o)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error marshalling qasino op to json for '%s': %s", o.Identity, err))
	}
	//os.Stdout.Write(json_string)

	auth := ""
	protocol := "http"
	// If no port, pick a default based on whether user/pass is set or not
	if port == 0 {
		if username != "" && password != "" {
			port = 443
		} else {
			port = 15597
		}
	}
	if port == 443 {
		// Must use auth on ssl port 443..
		auth = fmt.Sprintf("%s:%s@", username, password)
		protocol = "https"
	}

	url := fmt.Sprintf("%s://%s%s:%d/request?op=query", protocol, auth, hostname, port)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   (time.Duration(*timeout_s) * time.Second),
	}

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(json_string))

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed Post for qasino query for '%s': %s", o.Identity, err))
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error reading qasino response from query for '%s': %s\n", o.Identity, err))
	}

	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("Error response %d from qasino server for '%s': %s\n", resp.StatusCode, o.Identity, body))
	}

	// parse json into the response op structure.
	response_op := QasinoOpResponse{}
	err = json.Unmarshal(body, &response_op)
	if err != nil {
		errstr := fmt.Sprintf("Qasino response parse error for '%s': %s\n", o.Identity, err)
		//log.Printf("%s: Sent: %s  Received: %s\n", errstr, json_string, body)
		return nil, errors.New(errstr)

	} else if response_op.ResponseOp != "result_table" {

		errstr := fmt.Sprintf("Qasino response error: response_op is not result_table: %s", response_op.Message)
		// Log the entire json string sent for debugging
		//log.Printf("%s:  Sent: %s\n", errstr, json_string)
		return nil, errors.New(errstr)
	}

	// debug
	//log.Printf("Successfully queried: %s\n", body)
	return &response_op, nil
}
