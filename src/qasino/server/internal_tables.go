package server

import (
	qasinotable "qasino/table"
	"time"
)

// This insures all the internal tables are inserted using the same
// sql backend writer and makes sure that the "tables" table is called
// last (after all the other internal tables are added).

func (d *DataManager) insert_internal_tables(writer SqlReaderWriter, static SqlReaderWriter) {

	d.insert_info_table(writer)

	d.insert_connections_table(writer)

	d.insert_sql_stats_table(writer)

	d.insert_update_stats_table(writer)

	// This should be second to last so views can be created of any tables above.
	// This means though that you can not create views of any tables below.
	d.add_views(writer)

	d.insert_views_table(writer)

	// This should be last to include all the above tables.

	d.insert_tables_table(writer, static)
}

func (d *DataManager) insert_info_table(writer SqlReaderWriter) {
	table := qasinotable.NewTable("qasino_server_info")

	table.AddColumn("generation_number", "int")
	table.AddColumn("generation_duration_s", "int")
	table.AddColumn("generation_start_epoch", "int")

	table.AddRow(d.generation_number, d.generation_interval_s, d.generation_number)

	d.add_table_data(writer, table, d.self_identity)
}

// Adds a table (qasino_server_connections) to the database with per table info.
func (d *DataManager) insert_connections_table(writer SqlReaderWriter) {

	table := qasinotable.NewTable("qasino_server_connections")
	table.AddColumn("identity", "varchar")
	table.AddColumn("nr_tables", "int")
	table.AddColumn("last_update_epoch", "int")

	d.map_stats_mutex.Lock()
	for identity, connection_data := range d.connections {

		table.AddRow(
			identity,
			len(connection_data.tables),
			connection_data.last_update_ts.Unix(),
		)
	}
	d.map_stats_mutex.Unlock()

	d.add_table_data(writer, table, d.self_identity)
}

// Adds a status table (qasino_server_sql_stats) to the database in
// each generation.  Note we are actually saving stats from the
// "reader" backend because that is where sql stats are logged.
func (d *DataManager) insert_sql_stats_table(writer SqlReaderWriter) {

	table := qasinotable.NewTable("qasino_server_sql_stats")
	table.AddColumn("sql_received", "int")
	table.AddColumn("sql_completed", "int")
	table.AddColumn("sql_errors", "int")

	table.AddRow(
		d.stats.sql_received.Count(),
		d.stats.sql_completed.Count(),
		d.stats.sql_errors.Count(),
	)

	d.add_table_data(writer, table, d.self_identity)
}

// Adds a status table (qasino_server_update_stats) to the database in each generation.
func (d *DataManager) insert_update_stats_table(writer SqlReaderWriter) {
	table := qasinotable.NewTable("qasino_server_update_stats")

	table.AddColumn("updates_received", "int")
	table.AddColumn("updates_completed", "int")
	table.AddColumn("update_errors", "int")
	table.AddColumn("inserts_received", "int")
	table.AddColumn("inserts_completed", "int")

	table.AddRow(
		d.stats.updates_received.Count(),
		d.stats.updates_completed.Count(),
		d.stats.update_errors.Count(),
		d.stats.inserts_received.Count(),
		d.stats.inserts_completed.Count(),
	)

	d.add_table_data(writer, table, d.self_identity)
}

func bool_to_int(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}

// Adds a table (qasino_server_connections) to the database with per table info.
func (d *DataManager) insert_views_table(writer SqlReaderWriter) {
	table := qasinotable.NewTable("qasino_server_views")
	table.AddColumn("viewname", "varchar")
	table.AddColumn("loaded", "int")
	table.AddColumn("errormsg", "varchar")
	table.AddColumn("view", "varchar")

	for viewname, viewdata := range d.views {
		table.AddRow(
			viewname,
			bool_to_int(viewdata.loaded),
			viewdata.error,
			viewdata.view,
		)
	}

	d.add_table_data(writer, table, d.self_identity)
}

func (s *SqlBackend) add_tables_table_rows(table *qasinotable.Table, is_static bool) {

}

func (d *DataManager) insert_tables_table(writer SqlReaderWriter, static SqlReaderWriter) {

	table := qasinotable.NewTable("qasino_server_tables")
	table.AddColumn("tablename", "varchar")
	table.AddColumn("nr_rows", "int")
	table.AddColumn("nr_updates", "int")
	table.AddColumn("last_update_epoch", "int")
	table.AddColumn("static", "int")

	d.map_stats_mutex.Lock()
	for tablename, table_data := range d.tables {

		table.AddRow(
			tablename,
			table_data.nr_rows,
			table_data.updates,
			table_data.last_update_ts.Unix(),
			bool_to_int(table_data.is_static),
		)
	}
	d.map_stats_mutex.Unlock()

	// the chicken or the egg - how do we add ourselves?

	table.AddRow(
		"qasino_server_tables",
		table.GetNumRows()+1,
		1,
		time.Now().Unix(),
		0,
	)

	d.add_table_data(writer, table, d.self_identity)
}
