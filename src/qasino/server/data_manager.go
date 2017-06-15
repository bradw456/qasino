package server

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	qasinotable "qasino/table"
	"qasino/util"

	"github.com/golang/glog"
	"github.com/olekukonko/tablewriter"
	metrics "github.com/rcrowley/go-metrics"
)

var ShutdownChan chan struct{}

// By default we create in memory only sqlite dbs...  With a template
// filename, like "qasino_table_store_%d.db" the files can temporarily
// be staged to disk (there is usually no point though because they
// only last for the duration of a generation).  For legacy reasons
// (the python implementation needed to use files) we still support
// files.
const DEFAULT_BACKEND_FILENAME_TEMPLATE = ":memory:"

type ConnectionInfo struct {
	tables         map[string]int64
	last_update_ts time.Time
}

type ViewInfo struct {
	view   string
	loaded bool
	error  string
}

type SavedTableData struct {
	tablename string
	table     *qasinotable.Table
	identity  string
}

type Stats struct {
	inserts_received  metrics.StandardCounter
	inserts_completed metrics.StandardCounter
	updates_received  metrics.StandardCounter
	updates_completed metrics.StandardCounter
	update_errors     metrics.StandardCounter
	sql_received      metrics.StandardCounter
	sql_completed     metrics.StandardCounter
	sql_errors        metrics.StandardCounter
}

type TableInfo struct {
	updates        int
	nr_rows        int64
	last_update_ts time.Time
	is_static      bool
}

// DataManager is a singleton
type DataManager struct {
	self_identity string

	generation_interval_s     int
	sql_backend_writer        *SqlBackend
	sql_backend_reader        *SqlBackend
	sql_backend_writer_static *SqlBackend
	generation_number         int64

	signal_channel *SignalChannel

	// TODO read in from file
	views map[string]*ViewInfo

	saved_tables map[string]*SavedTableData

	stats Stats

	// Map of tablename to table info
	tables map[string]*TableInfo
	// Map of identity to connection info
	connections map[string]*ConnectionInfo

	map_stats_mutex sync.Mutex // covers the above two maps

	query_id_counter int64
}

func NewDataManager(interval int, identity string, signal_channel *SignalChannel) *DataManager {
	data_manager := &DataManager{
		self_identity:         identity,
		generation_interval_s: interval,
		generation_number:     time.Now().Unix(),
		sql_backend_writer: NewSqlBackend(
			util.ConfGetStringDefault("backend.filename_template", DEFAULT_BACKEND_FILENAME_TEMPLATE)),
		sql_backend_writer_static: NewSqlBackend(
			util.ConfGetStringDefault("backend.static_filename", "qasino_table_store_static.db")),
		sql_backend_reader: NewSqlBackend(
			util.ConfGetStringDefault("backend.filename_template", DEFAULT_BACKEND_FILENAME_TEMPLATE)),
		signal_channel: signal_channel,
		views:          make(map[string]*ViewInfo, 0),
		saved_tables:   make(map[string]*SavedTableData, 0),
		tables:         make(map[string]*TableInfo, 0),
		connections:    make(map[string]*ConnectionInfo, 0),
	}
	return data_manager
}

func (d *DataManager) Init() error {

	ShutdownChan = make(chan struct{}, 0)

	err := d.sql_backend_writer.db.Open(d.generation_number)
	if err != nil {
		glog.Infof("Error opening new writer for Init: %s\n", err)
		return err
	}

	d.sql_backend_writer.db.Use()

	err = d.sql_backend_writer_static.db.Open(0)
	if err != nil {
		glog.Infof("Error opening new static writer for Init: %s\n", err)
		return err
	}

	err = d.preload_tables_list(d.sql_backend_writer_static.db)
	if err != nil {
		glog.Warningf("Warning preloading tables in static db: %s\n", err)
	}

	err = d.sql_backend_reader.db.Open(1) // this is a throw-away
	if err != nil {
		glog.Infof("Error opening new reader for Init: %s\n", err)
		return err
	}

	d.attach_db(d.sql_backend_reader.db, d.sql_backend_writer_static.db)

	d.sql_backend_reader.db.Use()

	d.RotateDbs()

	go func() {
		generation_ticker := time.NewTicker(time.Duration(d.generation_interval_s) * time.Second)

		for {
			select {
			case <-generation_ticker.C:

				// TODO err check
				d.RotateDbs()

			case <-ShutdownChan:
				return
			}
		}
	}()

	return nil
}

func (d *DataManager) Shutdown() {

	close(ShutdownChan)

	if d.sql_backend_writer.db != nil {
		d.sql_backend_writer.db.Unuse()
	}
	if d.sql_backend_reader.db != nil {
		d.sql_backend_reader.db.Unuse()
	}
	// Don't unuse the static table or it will get deleted.
}

// This is used upon initial open of the db to get an actual list of
// tables from the db.
func (d *DataManager) preload_tables_list(db SqlReaderWriter) error {

	rows, err := db.Query("SELECT tbl_name FROM sqlite_master WHERE type = 'table' and tbl_name NOT LIKE 'sqlite_%'")

	if err != nil {
		return err
	}

	defer rows.Close()

	var tablename string
	tables := make(map[string]int, 0)

	for rows.Next() {

		err = rows.Scan(&tablename)
		if err != nil {
			return err
		}

		tables[tablename] = -1
	}
	if err != nil {
		return err
	}

	var nr_rows int

	for tablename, _ := range tables {

		row := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s;", tablename))

		err = row.Scan(&nr_rows)
		if err != nil {
			glog.Warningf("Failed to get number of rows for '%s': %s\n", tablename, err)
			continue
		}

		d.update_table_stats(tablename, int64(nr_rows), d.self_identity, false, true)
	}

	return nil
}

func (d *DataManager) attach_db(attach_to_db SqlReaderWriter, attach_from_db SqlReaderWriter) {
	if sqlite, ok := attach_from_db.(*SqliteBackend); ok {

		_, err := attach_to_db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS static;", sqlite.filename))

		if err != nil {
			glog.Warningf("Failed to attach database '%s': %s\n", sqlite.filename, err)
			return
		}
	}
}

func (d *DataManager) PrintStats() {
	glog.Infof("Stats: sql_received:      %d\n", d.stats.sql_received.Count())
	glog.Infof("Stats: sql_completed:     %d\n", d.stats.sql_completed.Count())
	glog.Infof("Stats: sql_errors:        %d\n", d.stats.sql_errors.Count())
	glog.Infof("Stats: updates_received:  %d\n", d.stats.updates_received.Count())
	glog.Infof("Stats: updates_completed: %d\n", d.stats.updates_completed.Count())
	glog.Infof("Stats: update_errors:     %d\n", d.stats.update_errors.Count())
	glog.Infof("Stats: inserts_received:  %d\n", d.stats.inserts_received.Count())
	glog.Infof("Stats: inserts_completed: %d\n", d.stats.inserts_completed.Count())
}

func (d *DataManager) reset_stats() {
	d.map_stats_mutex.Lock()
	for k, _ := range d.tables {
		delete(d.tables, k)
	}
	for k, _ := range d.connections {
		delete(d.connections, k)
	}
	d.map_stats_mutex.Unlock()

	d.stats = Stats{} // default values are all zero, which is what we want
}

func (d *DataManager) RotateDbs() {
	d.generation_number = time.Now().Unix()

	glog.Infof("Starting generation %d", d.generation_number)

	new_writer := d.sql_backend_writer.factory(util.ConfGetStringDefault("backend.filename_template", DEFAULT_BACKEND_FILENAME_TEMPLATE))

	err := new_writer.Open(d.generation_number)
	if err != nil {
		glog.Infof("Error opening new writer for generation %d: %s\n", d.generation_number, err)
		return
	}

	// This will get unused after two rotations (once to become a reader and once to get replaced as the reader).
	new_writer.Use()

	// new_writer doesn't need a 'Use' b/c we haven't swapped it in yet.. and static also doesn't not need a use.
	d.insert_internal_tables(new_writer, d.sql_backend_writer_static.db)

	d.PrintStats()

	d.reset_stats()

	// We have to do this everytime after the stats are reset
	// to get a new list of what is in the static database.
	// use counter not necessary for the static db.
	err = d.preload_tables_list(d.sql_backend_writer_static.db)
	if err != nil {
		glog.Warningf("Warning preloading tables in static db: %s\n", err)
	}

	// Add tables that had the "persist" option so we saved them for addtion in each new generation.
	d.add_saved_tables(new_writer)

	// Swap in the new writer under lock
	d.sql_backend_writer.db_mutex.Lock()

	saved_writer := d.sql_backend_writer.db
	d.sql_backend_writer.db = new_writer

	d.sql_backend_writer.db_mutex.Unlock()

	// Attach the static db.
	d.attach_db(saved_writer, d.sql_backend_writer_static.db)

	// The previous writer will be the new reader now.
	d.sql_backend_reader.db_mutex.Lock()

	saved_reader := d.sql_backend_reader.db
	d.sql_backend_reader.db = saved_writer

	d.sql_backend_reader.db_mutex.Unlock()

	// Finally release the reader.  This should usually trigger a
	// Close but if there are any active ops on it then when they
	// finish it should close (via UseCountCloser).
	saved_reader.Unuse()

	if d.signal_channel != nil {
		d.signal_channel.Send(d.generation_number, d.generation_interval_s, d.self_identity)
	}
}

func (d *DataManager) check_save_table(table *qasinotable.Table, identity string) {

	tablename := table.Tablename

	key := tablename + identity

	if table.GetBoolProperty("persist") {
		d.saved_tables[key] = &SavedTableData{
			table:     table,
			tablename: tablename,
			identity:  identity,
		}
	} else {
		// Be sure to remove a table that is no longer persisting.
		delete(d.saved_tables, key)
	}
}

func (d *DataManager) add_saved_tables(writer SqlReaderWriter) {

	for _, table_data := range d.saved_tables {

		glog.Infof("Adding saved table '%s' from '%s'", table_data.tablename, table_data.identity)

		d.add_table_data(writer, table_data.table, table_data.identity)
	}
}

// Add a table to the backend if it doesn't exist and insert the data.
func (d *DataManager) add_table_data(writer SqlReaderWriter, table *qasinotable.Table, identity string) error {

	var err error
	var rowcount int64
	var schema []*SchemaInfo
	var properties []string

	start_time := time.Now()

	tablename := table.Tablename

	logprefix := fmt.Sprintf("add_table_data (%s:%s)", identity, tablename)

	if tablename == "" {
		return util.Log_wrap_err("%s: invalid tablename", logprefix)
	}

	update := table.GetBoolProperty("update")
	static := table.GetBoolProperty("static")
	persist := table.GetBoolProperty("persist")

	// Check if we need to save the table in case of persistence.

	d.check_save_table(table, identity)

	// Now check if we've already added the table.

	if !update && !static && d.get_connection_table_rows(identity, tablename) > 0 {
		return util.Log_wrap_err("%s: already sent an update or is persisted for table", logprefix)
	}

	// Wrap the create, alter tables and inserts or updates in
	// a transaction... All or none please.

	tx, err := writer.Begin()

	if err != nil {
		return util.Log_wrap_err("%s: begin failed: %s", logprefix, err)
	}

	var create_table bytes.Buffer

	// Just always do a create table, relying on IF NOT EXISTS to
	// cover the case when another update beat us to it.

	create_table.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS '%s' ( \n", tablename))

	columns := make([]string, 0)

	for _, column_info := range table.ZipColumns() {
		columns = append(columns, fmt.Sprintf("'%s' %s DEFAULT NULL", column_info.Name, column_info.Type))
	}

	create_table.WriteString(strings.Join(columns, ",\n"))

	create_table.WriteString(");")

	_, err = tx.Exec(create_table.String())

	if err != nil {
		err = util.Log_wrap_err("%s: create table failed: %s", logprefix, err)
		goto rollback
	}

	// Merge the schemas of what might be already be there and our update.
	schema, err = d.get_schema_tx(tx, tablename)
	if err != nil {
		err = util.Log_wrap_err("%s: get_schema_tx failed: %s", logprefix, err)
		goto rollback
	}

	merge_table(tx, table, schema)

	// Now update the table.
	// Truncate and re-insert if this is static.
	// Call do_update if this is an update type.
	// Otherwise we insert.

	if static {
		_, err = tx.Exec(fmt.Sprintf("DELETE FROM '%s';", tablename))
		if err != nil {
			err = util.Log_wrap_err("%s: error deleting before insert for static table: %s", logprefix, err)
			goto rollback
		}
		rowcount, err = d.insert_table(tx, table)
		if err != nil {
			err = util.Log_wrap_err("%s: error inserting static table: %s", logprefix, err)
			goto rollback
		}
	} else {
		if update {
			rowcount, err = d.update_table(tx, table, identity)
			if err != nil {
				err = util.Log_wrap_err("%s: error updating table: %s", logprefix, err)
				goto rollback
			}

		} else {
			rowcount, err = d.insert_table(tx, table)
			if err != nil {
				err = util.Log_wrap_err("%s: error inserting regular table: %s", logprefix, err)
				goto rollback
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		err = util.Log_wrap_err("%s: commit failed: %s", logprefix, err)
		goto rollback
	}

	//nr_rows := len(table.Rows)

	properties = make([]string, 0)
	if static {
		properties = append(properties, " static")
	}
	if update {
		properties = append(properties, " update")
	}
	if persist {
		properties = append(properties, " persist")
	}

	glog.V(1).Infof("New data for%s table '%s' %d rows from '%s' (%s)",
		strings.Join(properties, ""), tablename, rowcount, identity, time.Since(start_time))

	// Update some informational stats used for making the tables
	// and connections tables.

	d.update_table_stats(tablename, rowcount, identity, (!update && !static), static)

	return nil

rollback:
	tx.Rollback()

	// TODO retries?
	return err

} // add_table_data

func (d *DataManager) update_table_stats(tablename string, nr_rows int64, identity string, do_sum bool, is_static bool) {

	now := time.Now()

	d.map_stats_mutex.Lock()
	defer d.map_stats_mutex.Unlock()

	// Keep track of how many updates a table has received.

	if entry, found := d.tables[tablename]; found {

		if do_sum {
			entry.nr_rows += nr_rows
		}

		entry.updates++
		entry.last_update_ts = now
		entry.is_static = is_static

	} else {

		d.tables[tablename] = &TableInfo{
			updates:        1,
			nr_rows:        nr_rows,
			last_update_ts: now,
			is_static:      is_static,
		}
	}

	// Keep track of which "identities" have added to a table.

	// Except for static tables

	if is_static {
		return
	}

	if conn_entry, found := d.connections[identity]; found {

		conn_entry.last_update_ts = now

		_, found2 := conn_entry.tables[tablename]
		if do_sum && found2 {
			conn_entry.tables[tablename] += nr_rows
		} else {
			conn_entry.tables[tablename] = nr_rows
		}

	} else {
		ci := &ConnectionInfo{
			tables:         make(map[string]int64, 0),
			last_update_ts: now,
		}
		ci.tables[tablename] = nr_rows
		d.connections[identity] = ci
	}
} // update_table_stats

// TODO sqlite specific
func (d *DataManager) update_table(tx *sql.Tx, table *qasinotable.Table, identity string) (int64, error) {

	var err error

	d.stats.updates_received.Inc(1)

	key_cols_str := table.Properties.KeyCols

	column_names := table.ColumnNames

	key_column_names := make([]string, 0)
	if key_cols_str == "" {
		key_column_names = column_names
	} else {
		key_column_names = strings.Split(key_cols_str, ";")
	}

	tablename := table.Tablename
	index_name := fmt.Sprintf("%s_unique_index_%s", tablename, strings.Join(key_column_names, "_"))

	create_index_sql := fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (%s)",
		index_name, tablename, strings.Join(key_column_names, ", "))

	glog.V(2).Infof("DEBUG: update_table: would execute: %s\n", create_index_sql)

	_, err = tx.Exec(create_index_sql)

	if err != nil {
		d.stats.update_errors.Inc(1)
		return 0, util.Wrap_err("update_table: failed to create unique index for table update: %s: ( %s )", err, create_index_sql)
	}

	sql := fmt.Sprintf("INSERT OR REPLACE INTO '%s' (%s) VALUES (%s)",
		tablename, strings.Join(column_names, ", "), util.RepeatJoin("?", ", ", len(column_names)))

	var rowcount int64

	for _, row := range table.Rows {

		glog.V(2).Infof("DEBUG: update_table: would execute: %s with %+v\n", sql, row)

		result, err := tx.Exec(sql, []interface{}{row}...)
		if err != nil {
			return 0, util.Wrap_err("update_table: error on insert or replace: %s", err)
		}

		nr_rows, err := result.RowsAffected()
		if err != nil {
			return 0, util.Wrap_err("update_table: could not get rows affected: %s", err)
		}

		rowcount += nr_rows
	}

	d.stats.updates_completed.Inc(1)

	return rowcount, nil

} // update_table

func ConvertToInt(value interface{}) (result int64, err error, warning error) {
	switch value.(type) {
	case int:
		return int64(value.(int)), nil, nil
	case int32:
		return int64(value.(int32)), nil, nil
	case int64:
		return value.(int64), nil, nil
	case float32:
		// Check if its a whole number.
		f := value.(float32)
		if f == float32(int32(f)) {
			return int64(f), nil, nil
		} else {
			return int64(f), nil, errors.New("truncating float to integer")
		}
		//return int64(math.Floor(float64(value.(float32)) + 0.5)), nil, errors.New("truncating float to integer")
	case float64:
		// Check if its a whole number.
		f := value.(float64)
		if f == float64(int64(f)) {
			return int64(f), nil, nil
		} else {
			return int64(f), nil, errors.New("truncating float to integer")
		}
	case string:
		value_int, err := strconv.ParseInt(value.(string), 10, 64)
		if err != nil {
			return 0, err, nil
		}
		return value_int, nil, nil
	}
	return 0, errors.New(fmt.Sprintf("Unsupported type to convert to integer (%T)", value)), nil
}

// Insert table into the backend.  Use multi-row insert statements
// (supported in sqlite 3.7.11) for speed.
func (d *DataManager) insert_table(tx *sql.Tx, table *qasinotable.Table) (int64, error) {

	var err error

	d.stats.inserts_received.Inc(1)

	var rowcount int64

	base_query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table.Tablename, strings.Join(table.ColumnNames, ","))

	nr_columns := len(table.ColumnNames)
	max_nr_values := 400 // supposedly the max is 500..

	nr_values := 0
	bind_values := make([]interface{}, 0)

	sql := bytes.NewBufferString(base_query)

	one_row := "(" + util.RepeatJoin("?", ",", nr_columns) + ")"

	for row_index, row := range table.Rows {

		if nr_values+nr_columns > max_nr_values {

			// execute this batch and start again.

			sql.WriteString(util.RepeatJoin(one_row, ",", (nr_values / nr_columns)))

			//glog.V(2).Infof("DEBUG: hit limit: executing (values %d, rows %d): %s", nr_values, rowcount, sql)

			_, err = tx.Exec(sql.String(), bind_values...)
			if err != nil {
				return 0, util.Wrap_err("insert_table: failed to execute insert sql for '%s': %s", table.Tablename, err)
			}
			bind_values = make([]interface{}, 0)
			nr_values = 0
			sql.Reset()
			sql.WriteString(base_query)
		}

		nr_values += nr_columns

		// add this row to our bind values

		for column_index, value := range row {
			switch table.ColumnTypes[column_index] {
			case "int":
				value_int, err, warn := ConvertToInt(value)
				if warn != nil {
					// So encoding/json will parse the "numeric" JSON type into a float
					// Even if we're expecting an Integer the type will be float.
					// The conversion is supposed to tell when the float is not a whole number.
					// If you get lots of warnings here this might be why (somebody sending
					// json numeric fields with floating point components under a column that
					// should be ints).
					glog.Warningf(fmt.Sprintf("insert_table: conversion to int warning in '%s' row=%d col=%d: %s",
						table.Tablename, row_index, column_index, warn))
				}
				if err != nil {
					return 0, util.Wrap_err("insert_table: conversion to int failed in '%s' row=%d col=%d: %s",
						table.Tablename, row_index, column_index, err)
				}
				bind_values = append(bind_values, value_int)
			default:
				bind_values = append(bind_values, fmt.Sprintf("%v", value))
			}
		}

		rowcount += 1
	}

	// handle last batch
	if rowcount > 0 {

		sql.WriteString(util.RepeatJoin(one_row, ",", (nr_values / nr_columns)))

		//glog.V(1).Infof("DEBUG: final: executing (values %d, rows %d): %s", nr_values, rowcount, sql)

		_, err = tx.Exec(sql.String(), bind_values...)
		if err != nil {
			return 0, util.Wrap_err("insert_table: failed to execute insert sql for '%s': %s", table.Tablename, err)
		}
	}

	d.stats.inserts_completed.Inc(1)

	return rowcount, nil
}

func (d *DataManager) get_schema_tx(tx *sql.Tx, tablename string) ([]*SchemaInfo, error) {
	// TODO abstract this pragma
	rows, err := tx.Query(fmt.Sprintf("pragma table_info( '%s' );", tablename))
	if err != nil {
		return nil, util.Wrap_err("get_schema: pragma table_info failed on %s: %s", tablename, err)
	}

	schema := make([]*SchemaInfo, 0)

	defer rows.Close()
	for rows.Next() {
		var index int
		schemainfo := &SchemaInfo{}
		err = rows.Scan(&index, &schemainfo.Name, &schemainfo.Type)
		schema = append(schema, schemainfo)
	}
	err = rows.Err() // get any error encountered during iteration

	if err != nil {
		return nil, util.Wrap_err("get_schema: rows error on %s: %s", tablename, err)
	}
	return schema, nil
}

func (d *DataManager) get_schema(reader SqlReaderWriter, tablename string) ([]*SchemaInfo, error) {
	// TODO abstract this pragma
	rows, err := reader.Query(fmt.Sprintf("pragma table_info( '%s' );", tablename))
	if err != nil {
		return nil, util.Wrap_err("get_schema: pragma table_info failed on %s: %s", tablename, err)
	}

	schema := make([]*SchemaInfo, 0)

	defer rows.Close()
	for rows.Next() {
		var index int
		var rest1, rest2, rest3 interface{}
		schemainfo := &SchemaInfo{}
		err = rows.Scan(&index, &schemainfo.Name, &schemainfo.Type, &rest1, &rest2, &rest3)
		if err != nil {
			return nil, util.Wrap_err("get_schema: scan error on %s: %s", tablename, err)
		}
		schema = append(schema, schemainfo)
	}
	err = rows.Err() // get any error encountered during iteration

	if err != nil {
		return nil, util.Wrap_err("get_schema: rows error on %s: %s", tablename, err)
	}
	return schema, nil
}

func (d *DataManager) get_connection_table_rows(identity, tablename string) int64 {
	d.map_stats_mutex.Lock()
	defer d.map_stats_mutex.Unlock()

	if entry, found := d.connections[identity]; found {

		if nr_rows, found2 := entry.tables[tablename]; found2 {

			return nr_rows
		}
	}
	return 0
}

// Add all views to the backend.  This should not take a long time.
func (d *DataManager) add_views(writer SqlReaderWriter) {

	var err error

	for viewname, viewdata := range d.views {

		glog.Infof("Adding view '%s'\n", viewname)

		_, err = writer.Exec(viewdata.view)
		if err != nil {
			glog.Errorf("Failed to add view '%s': %s", viewname, err)
			viewdata.loaded = false
			viewdata.error = err.Error()
		} else {
			viewdata.loaded = true
			viewdata.error = ""
		}
	}
}

type SelectResultData struct {
	ColumnNames []string   `json:"column_names"`
	Rows        [][]string `json:"rows"`
}

type SelectResult struct {
	Data SelectResultData
	// key=ColumnIndex, value=MaxWidth. Using string->int b/c this will be marshalled into json ...
	MaxWidths map[string]int
}

func (s *SelectResult) String() string {
	outbuf := &bytes.Buffer{}

	table := tablewriter.NewWriter(outbuf)
	table.SetHeader(s.Data.ColumnNames)
	table.SetBorder(false)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	//table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.SetCenterSeparator(" ")
	table.SetColumnSeparator(" ")

	string_rows := make([][]string, len(s.Data.Rows))

	for row_idx, row := range s.Data.Rows {
		string_rows[row_idx] = make([]string, len(row))
		for cell_idx, cell := range row {
			string_rows[row_idx][cell_idx] = fmt.Sprintf("%v", cell)
		}
	}

	table.AppendBulk(string_rows)
	table.Render()

	outbuf.WriteString(fmt.Sprintf("\n%d rows returned\n", len(s.Data.Rows)))

	return outbuf.String()
}

func (s *SelectResult) CSV() string {
	outbuf := &bytes.Buffer{}

	writer := csv.NewWriter(outbuf)

	writer.Write(s.Data.ColumnNames)

	row_str := make([]string, len(s.Data.ColumnNames))

	for _, row := range s.Data.Rows {

		for i := 0; i < len(s.Data.ColumnNames); i++ {
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

func (s *SelectResult) HTML() string {
	var outbuf bytes.Buffer

	outbuf.WriteString("<table>\n  <thead><tr>")

	for _, column_name := range s.Data.ColumnNames {
		outbuf.WriteString(fmt.Sprintf("<th>%s</th>", column_name))
	}
	outbuf.WriteString("</tr></thead>\n  <tbody>\n")

	for _, row := range s.Data.Rows {
		outbuf.WriteString("  <tr>")
		for _, cell := range row {
			outbuf.WriteString(fmt.Sprintf("<td>%v</td>", cell))
		}
		outbuf.WriteString("</tr>\n")
	}

	outbuf.WriteString("  </tbody>\n</table>")

	return outbuf.String()
}

func (d *DataManager) do_select(reader SqlReaderWriter, query_id int64, sql_statement string) (*SelectResult, error) {

	var err error

	d.stats.sql_received.Inc(1)

	var rows *sql.Rows
	rows, err = reader.Query(sql_statement)

	if err != nil {
		d.stats.sql_errors.Inc(1)
		return nil, err
	}

	defer rows.Close()

	// Find the max column with for each column.

	max_widths := make(map[string]int, 0)

	// Get the column names.

	column_names, err := rows.Columns()

	if err != nil {
		d.stats.sql_errors.Inc(1)
		return nil, err

		/*
			# Ugh, getdescription fails if the query succeeds but returns no rows!  This is the message:
			#    "Can't get description for statements that have completed execution"

			# For now return a zero row empty table:

			data = { "column_names" : [ "query returned zero rows" ], "rows" : [ ] }

			return { "retval" : 0, "error_message" : '', "data" : data, "max_widths" : { "0" : 24 } }
		*/
	}

	set_max_width := func(column_index int, length int) {
		ci := strconv.FormatInt(int64(column_index), 10)
		if width, has := max_widths[ci]; has {
			if width < length {
				max_widths[ci] = length
			}
		} else {
			max_widths[ci] = length
		}

	}

	for column_index, column_name := range column_names {

		set_max_width(column_index, len(column_name))
	}

	// For each row.

	saved_rows := make([][]string, 0)

	for rows.Next() {

		// ??? TODO - ???: Under certain circustances (like 'select 1; abc') we'll get an exception in fetchall!

		// yeah....
		values := make([]interface{}, len(column_names))
		value_ptrs := make([]interface{}, len(column_names))

		for i, _ := range column_names {
			value_ptrs[i] = &values[i]
		}

		err = rows.Scan(value_ptrs...)
		if err != nil {
			errorstr := fmt.Sprintf("Error getting sqlite rows: %s\n", err)
			glog.Errorln(errorstr)
			return nil, errors.New(errorstr)
		}
		row := make([]string, len(values))

		// there has got to be a better way...
		for i, val := range values {
			set := false
			switch val.(type) {
			case []byte:
				set = true
				row[i] = string(val.([]byte))
			}
			if !set {
				// let go figure it out
				row[i] = fmt.Sprintf("%v", val)
			}

			//errorstr := fmt.Sprintf("Error converting value to string value=%v type=%T\n", val, val)
			//glog.Errorln(errorstr)
			//return nil, errors.New(errorstr)

			set_max_width(i, len(row[i]))
		}

		saved_rows = append(saved_rows, row)
	}

	d.stats.sql_completed.Inc(1)

	return &SelectResult{
		Data: SelectResultData{
			ColumnNames: column_names,
			Rows:        saved_rows,
		},
		MaxWidths: max_widths,
	}, nil

} // do_select

func (d *DataManager) do_desc(reader SqlReaderWriter, query_id int64, tablename string) (*SelectResult, error) {

	glog.Infof("DEBUG: do desc %s \n", tablename)
	schema, err := d.get_schema(reader, tablename)
	if err != nil {
		glog.Errorf("do_desc: %d failed to get schema: %s", query_id, err)
		return nil, errors.New(fmt.Sprintf("Table %s not found", tablename))
	}

	rows := make([][]string, len(schema))
	for i, item := range schema {
		rows[i] = []string{item.Name, item.Type}
	}

	return &SelectResult{
		Data: SelectResultData{
			ColumnNames: []string{"column_name", "column_type"},
			Rows:        rows,
		},
	}, nil
}

func (d *DataManager) get_query_id() int64 {
	return atomic.AddInt64(&d.query_id_counter, 1)
}

func (d *DataManager) process_non_select(reader SqlReaderWriter, query_id int64, sql_statement string) (*SelectResult, error) {

	var matches []string

	// DESC <TABLENAME>?

	desc_table := regexp.MustCompile(`^\s*desc\s+(\S+)\s*;$`)
	matches = desc_table.FindStringSubmatch(sql_statement)
	if matches != nil {
		return d.do_desc(reader, query_id, matches[1])
	}

	// DESC VIEW <VIEWNAME>?

	desc_view := regexp.MustCompile(`^\s*desc\s+view\s+(\S+)\s*;$`)
	matches = desc_view.FindStringSubmatch(sql_statement)
	if matches != nil {
		return d.do_select(reader, query_id, fmt.Sprintf("SELECT view FROM qasino_server_views WHERE viewname = '{}';", matches[1]))
	}

	// SHOW tables?

	show_tables := regexp.MustCompile(`^\s*show\s+tables\s*;$`)
	if show_tables.MatchString(sql_statement) {

		return d.do_select(reader, query_id, "SELECT *, strftime('%Y-%m-%d %H:%M:%f UTC', last_update_epoch, 'unixepoch') last_update_datetime FROM qasino_server_tables ORDER BY tablename;")
	}

	// SHOW tables with LIKE?

	show_tables_like := regexp.MustCompile(`^\s*show\s+tables\s+like\s+('\S+')\s*;$`)
	matches = show_tables_like.FindStringSubmatch(sql_statement)

	if matches != nil {
		return d.do_select(reader, query_id, fmt.Sprintf("SELECT *, strftime('%Y-%m-%d %H:%M:%f UTC', last_update_epoch, 'unixepoch') last_update_datetime FROM qasino_server_tables WHERE tablename LIKE %s ORDER BY tablename;", matches[1]))
	}

	// SHOW connections?

	show_connections := regexp.MustCompile(`^\s*show\s+connections\s*;$`)
	if show_connections.MatchString(sql_statement) {

		return d.do_select(reader, query_id, "SELECT *, strftime('%Y-%m-%d %H:%M:%f UTC', last_update_epoch, 'unixepoch') last_update_datetime FROM qasino_server_connections ORDER BY identity;")
	}

	// SHOW info?

	show_info := regexp.MustCompile(`^\s*show\s+info\s*;$`)
	if show_info.MatchString(sql_statement) {

		return d.do_select(reader, query_id, "SELECT *, strftime('%Y-%m-%d %H:%M:%f UTC', generation_start_epoch, 'unixepoch') generation_start_datetime FROM qasino_server_info;")
	}

	// SHOW views?

	show_views := regexp.MustCompile(`^\s*show\s+views\s*;$`)
	if show_views.MatchString(sql_statement) {

		return d.do_select(reader, query_id, "SELECT viewname, loaded, errormsg FROM qasino_server_views ORDER BY viewname;")
	}

	// Exit?

	quit := regexp.MustCompile(`^\s*(quit|logout|exit)\s*;$`)
	if quit.MatchString(sql_statement) {
		return nil, nil
	}

	return nil, errors.New("ERROR: Unrecognized statement")
}
