package server

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang/glog"
)

// Monolithic handler for all POSTs on /request path.
func LegacyHandlePOST_request(c *gin.Context, d *DataManager) {

	op_name := c.DefaultQuery("op", "")

	var op RequestOp
	var err error

	//body, _ := ioutil.ReadAll(c.Request.Body)
	//fmt.Printf("BODY: %s\n", string(body))
	//defer c.Request.Body.Close()

	err = c.BindJSON(&op)
	if err != nil {
		errmsg := fmt.Sprintf("Invalid input format in POST body: %s", err)
		glog.Errorln(errmsg)
		// This seems to actually return 400... hmmm
		c.JSON(200, ErrorResponse(errmsg, d.self_identity))
		return
	}

	// If no op name in query string param use whats in the post json.
	if op_name == "" {
		op_name = op.Op
	}

	switch op_name {
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
			glog.Errorf("Error from add_table_data: %s", err)
			c.JSON(200, ErrorResponse(err.Error(), d.self_identity))
			return
		}

		c.JSON(200, BasicSuccessResponse(d.self_identity))
		return
	case "query":

		if op.Sql == "" {
			glog.Errorf("Query Op is missing 'sql'\n")
			c.JSON(200, ErrorResponse("Must specify 'sql' param", d.self_identity))
			return
		}

		result, err := process_sql_statement(op.Sql, d)
		if err != nil {
			errmsg := fmt.Sprintf("Error processing sql: %s: %s", err, op.Sql)
			glog.Errorln(errmsg)
			c.JSON(200, ErrorResponse(errmsg, d.self_identity))
			return
		}

		c.JSON(200, DataSuccessResponse(result, d.self_identity))
		return

	default:
		errmsg := fmt.Sprintf("Unrecognized operation: '%s'", op_name)
		glog.Errorln(errmsg)
		c.JSON(200, ErrorResponse(errmsg, d.self_identity))
		return
	}
}

// Monolithic handler for all GETs on /request path.
func LegacyHandleGET_request(c *gin.Context, d *DataManager) {

	op_name := c.DefaultQuery("op", "")

	//var err error

	switch op_name {
	case "query":

		sql := c.DefaultQuery("sql", "")
		format := c.DefaultQuery("format", "json")

		switch format {
		case "json", "text", "csv", "html":
		default:
			errmsg := "Query format is invalid, must be 'json', 'text', 'csv', or 'html'"
			glog.Errorln(errmsg)
			c.JSON(200, ErrorResponse(errmsg, d.self_identity))
			return
		}

		//fmt.Printf("Got sql: %s\n", sql)

		if sql == "" {
			glog.Errorf("Query Op is missing 'sql'\n")
			c.JSON(200, ErrorResponse("Must specify 'sql' param", d.self_identity))
			return
		}

		result, err := process_sql_statement(sql, d)
		if err != nil {
			errmsg := fmt.Sprintf("Error processing sql: %s: %s", err, sql)
			glog.Errorln(errmsg)
			c.JSON(200, ErrorResponse(errmsg, d.self_identity))
			return
		}

		// TODO use c.Writer...

		switch format {
		case "json":
			c.JSON(200, DataSuccessResponse(result, d.self_identity))
		case "text":
			c.String(200, result.String())
		case "csv":
			c.String(200, result.CSV())
		case "html":
			c.String(200, result.HTML())
		}
		return

	default:
		errmsg := fmt.Sprintf("Unrecognized operation : '%s'", op_name)
		glog.Errorln(errmsg)
		c.JSON(200, ErrorResponse(errmsg, d.self_identity))
		return
	}
}

func process_sql_statement(sql_statement string, d *DataManager) (*SelectResult, error) {
	query_id := d.get_query_id()

	query_start := time.Now()

	glog.Infof("http_receiver: (%d) SQL received: %s\n", query_id, sql_statement)

	var err error = nil
	var result *SelectResult

	d.sql_backend_reader.Use(func(reader SqlReaderWriter) {

		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(sql_statement)), "select") {

			// Select statement
			result, err = d.do_select(reader, query_id, sql_statement)

		} else {

			result, err = d.process_non_select(reader, query_id, sql_statement)
		}
	})

	resultstr := "success"
	if err != nil {
		resultstr = "failure"
	}

	glog.Infof("http_receiver: (%d) SQL completed (%s) result is %s\n", query_id, time.Since(query_start), resultstr)

	return result, err
}

func UIHandleGET_tables(c *gin.Context, d *DataManager) {

	//        <td><a href="{{ links[column_names[loop.index0]] | format(cell) }}">{{ cell }}</a></td>

	tmpl := "tables.html.tmpl"

	sql := "select tablename, nr_rows, nr_updates, datetime(last_update_epoch, 'unixepoch') last_update, static from qasino_server_tables order by tablename;"

	column_names := []string{"Tablename", "Nr Rows", "Nr Updates", "Last Update", "Static"}

	result, err := process_sql_statement(sql, d)

	if err != nil {
		c.HTML(http.StatusOK, tmpl, gin.H{
			"page":          "tables",
			"error_message": fmt.Sprintf("Error executing SQL: %s", err),
		})
		return
	}

	c.HTML(http.StatusOK, tmpl, gin.H{
		"page":          "tables",
		"error_message": "",
		"column_names":  column_names,
		"rows":          result.Data.Rows,
	})
}

func UIHandleGET_desc(c *gin.Context, d *DataManager) {

	tmpl := "desc.html.tmpl"

	tablename := c.DefaultQuery("tablename", "")

	if tablename != "" {
		sql := fmt.Sprintf("desc %s;", tablename)

		column_names := []string{"Column Name", "Column Type"}

		result, err := process_sql_statement(sql, d)

		if err != nil {
			c.HTML(http.StatusOK, tmpl, gin.H{
				"page":          "desc",
				"error_message": fmt.Sprintf("Error executing DESC: %s", err),
			})
			return
		}

		c.HTML(http.StatusOK, tmpl, gin.H{
			"page":          "desc",
			"error_message": "",
			"tablename":     tablename,
			"column_names":  column_names,
			"rows":          result.Data.Rows,
		})
		return
	}

	c.HTML(http.StatusOK, tmpl, gin.H{
		"page":          "desc",
		"error_message": "",
	})
}

func UIHandleGET_query(c *gin.Context, d *DataManager) {

	tmpl := "query.html.tmpl"

	sql := c.DefaultQuery("sql", "")

	if sql != "" {

		result, err := process_sql_statement(sql, d)

		if err != nil {
			c.HTML(http.StatusOK, tmpl, gin.H{
				"page":          "query",
				"error_message": fmt.Sprintf("Error executing SQL: %s", err),
			})
			return
		}

		c.HTML(http.StatusOK, tmpl, gin.H{
			"page":          "query",
			"error_message": "",
			"sql":           sql,
			"column_names":  result.Data.ColumnNames,
			"rows":          result.Data.Rows,
		})
		return
	}

	c.HTML(http.StatusOK, tmpl, gin.H{
		"page":          "query",
		"error_message": "",
		"sql":           sql,
	})
}
