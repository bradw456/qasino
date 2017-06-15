package server

import (
	"database/sql"
	"fmt"
	qasinotable "qasino/table"

	"github.com/golang/glog"

	// consider using this? "deckarep/golang-set"
)

type SchemaInfo struct {
	Name string
	Type string
}

func merge_table(tx *sql.Tx, table_to_add *qasinotable.Table, existing_schema []*SchemaInfo) {

	// Determine if we need to merge this table.

	tablename := table_to_add.Tablename

	// Make some sets

	table_to_add_columns := make(map[string]struct{}, 0)
	existing_columns := make(map[string]struct{}, 0)

	for _, c := range table_to_add.ColumnNames {
		table_to_add_columns[c] = struct{}{}
	}

	for _, s := range existing_schema {
		existing_columns[s.Name] = struct{}{}
	}

	// Find discrepancies between table and existing schema.

	// First, are there new columns in the table to add?

	columns_to_add := make([]string, 0)

	for c, _ := range table_to_add_columns {
		if _, found := existing_columns[c]; found {
			columns_to_add = append(columns_to_add, c)
		}
	}

	if len(columns_to_add) > 0 {

		// The existing table we are adding to is missing columns.
		// Try to add the missing columns to the table.

		column_type_lookup := make(map[string]string, 0)
		for _, column_info := range table_to_add.ZipColumns() {
			column_type_lookup[column_info.Name] = column_info.Type
		}

		for _, column_name := range columns_to_add {
			column_type, found := column_type_lookup[column_name]
			if found {
				sql := fmt.Sprintf("ALTER TABLE '%s' ADD COLUMN '%s' %s DEFAULT NULL;", tablename, column_name, column_type)

				glog.Infof("TableMerger: Altering table %s to add column %s %s", tablename, column_name, column_type)
				_, err := tx.Exec(sql)

				if err != nil {
					glog.Errorf("TableMerger: Error altering table: %s\n", err)
				}
			}
		}
	}

	// Else we may have columns that are missing from the table to
	// add (ie in the existing schema but not in the new table to
	// add).  We do nothing though because the insert will just not
	// insert those columns and we assume default values for all
	// columns.

	// TODO type changes.
}
