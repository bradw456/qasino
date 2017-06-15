package server

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/spf13/viper"

	qasinotable "qasino/table"
)

func FailFatal(t *testing.T, format string, msg ...interface{}) {
	log.Printf(format, msg...)

	n := 0
	buf := make([]byte, 1024)
	for {
		n = runtime.Stack(buf, false)
		if n < len(buf) {
			buf[n] = 0
			break
		}
		buf = make([]byte, len(buf)*2)
	}
	fmt.Printf("Current stack:\n\n %s\n", string(buf[0:n]))

	os.Exit(1)
}

func GetTestDataManager(t *testing.T) *DataManager {
	viper.Set("backend.static_filename", "tmp_unit_test_static.db")

	// Use a long generation so we do our own rotation triggering.

	d := NewDataManager(3600, "testident", nil)

	err := d.Init()
	if err != nil {
		FailFatal(t, "GetTestDataManager failed: %s\n", err)
	}

	return d
}

func Shutdown(d *DataManager) {
	d.Shutdown()
	os.Remove("tmp_unit_test_static.db")
}

func GetDummyTable() *qasinotable.Table {

	table := qasinotable.NewTable("dummy_test_table")

	table.AddColumn("columnA", "int")
	table.AddColumn("columnB", "text")
	table.AddColumn("columnC", "real")

	// Add in 'order by 1' order.
	table.AddRow(1122, "The quick brown fox", 3.14159265)
	table.AddRow(987654321, "jumped over the lazy dog", 0.00001)

	return table
}

func GetDummyTableStatic() *qasinotable.Table {
	table := GetDummyTable()
	table.SetProperty("static", true)
	return table
}

func GetDummyTablePersist() *qasinotable.Table {
	table := GetDummyTable()
	table.SetProperty("persist", true)
	return table
}

func PublishDummyTable(d *DataManager, db SqlReaderWriter, table *qasinotable.Table, t *testing.T) {

	err := d.add_table_data(db, table, d.self_identity)
	if err != nil {
		FailFatal(t, "PublishDummyTable failed: %s\n", err)
	}
}

// Not using "String" b/c we're just looking at data.
func SelectResultAsString(s *SelectResultData) string {
	var buf bytes.Buffer
	buf.WriteString("Columns: ")
	for _, column := range s.ColumnNames {
		buf.WriteString(" " + column)
	}
	buf.WriteString("\nRows:\n")
	for _, row := range s.Rows {
		for _, cell := range row {
			buf.WriteString(fmt.Sprintf(" %v", cell))
		}
		buf.WriteString("\n")
	}
	return buf.String()
}

func StringArraysEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, ai := range a {
		if ai != b[i] {
			return false
		}
	}
	return true
}

func ArraysEqual(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i, ai := range a {
		if ai != b[i] {
			return false
		}
	}
	return true
}

func CompareSelectResultAndTable(result *SelectResultData, table *qasinotable.Table) int {
	if !StringArraysEqual(result.ColumnNames, table.ColumnNames) {
		fmt.Printf("columns not equal '%v' != '%v'\n", result.ColumnNames, table.ColumnNames)
		return 1
	}
	if len(result.Rows) != len(table.Rows) {
		fmt.Printf("nr row not equal %d != %d\n", len(result.Rows), len(table.Rows))
		return 1
	}
	for row_index, row := range result.Rows {
		if len(row) != len(table.Rows[row_index]) {
			fmt.Printf("row %d lengths not equal %d != %d\n", len(row), len(table.Rows[row_index]))
			return 1
		}
		for col_index, col := range row {
			// Compare the string representation for everything b/c tables are usually all strings
			if fmt.Sprintf("%v", col) != fmt.Sprintf("%v", table.Rows[row_index][col_index]) {
				fmt.Printf("row %d col %d: %v != %v\n",
					row_index, col_index, col, table.Rows[row_index][col_index])
				return 1
			}
		}
	}

	return 0
}

func QueryDummyTable(d *DataManager, db SqlReaderWriter, expected_table *qasinotable.Table, t *testing.T) {
	result, err := d.do_select(db, 1, "SELECT * FROM dummy_test_table ORDER BY 1")

	if err != nil {
		FailFatal(t, "QueryDummyData failed: %s\n", err)
	}

	if CompareSelectResultAndTable(&result.Data, expected_table) != 0 {
		FailFatal(t, "QueryDummyTable results from query are not expected: query ===>\n%s <=== != table ===>\n%s <===\n",
			SelectResultAsString(&result.Data), expected_table.String())
	}

}

func QueryDummyTableExpectMissing(d *DataManager, db SqlReaderWriter, t *testing.T) {
	_, err := d.do_select(db, 1, "SELECT * FROM dummy_test_table ORDER BY 1")

	if err == nil {
		FailFatal(t, "QueryDummyDataExpectMissing, query succeeded!\n")
	}
	if err.Error() != "no such table: dummy_test_table" {
		FailFatal(t, "QueryDummyDataExpectMissing failed: %s\n", err)
	}
}

func TestBasicTablePublish(t *testing.T) {

	d := GetTestDataManager(t)

	table := GetDummyTable()

	// Publish table

	PublishDummyTable(d, d.sql_backend_writer.db, table, t)

	// Check its there in the writer db

	QueryDummyTable(d, d.sql_backend_writer.db, table, t)

	// Rotate the dbs

	time.Sleep(2 * time.Second)
	d.RotateDbs()

	// Check its not in the writer db now

	QueryDummyTableExpectMissing(d, d.sql_backend_writer.db, t)

	// But is in the reader db

	QueryDummyTable(d, d.sql_backend_reader.db, table, t)

	// Rotate again

	time.Sleep(2 * time.Second)
	d.RotateDbs()

	// Make sure its no longer in the reader

	QueryDummyTableExpectMissing(d, d.sql_backend_reader.db, t)

	Shutdown(d)
}

func TestStaticTablePublish(t *testing.T) {

	d := GetTestDataManager(t)

	table := GetDummyTableStatic()

	// Publish table

	PublishDummyTable(d, d.sql_backend_writer_static.db, table, t)

	// Check its there in the static db.

	QueryDummyTable(d, d.sql_backend_writer_static.db, table, t)

	// Rotate the dbs

	time.Sleep(2 * time.Second)
	d.RotateDbs()

	// Check its in the reader db (which should be "attached" to the static db)

	QueryDummyTable(d, d.sql_backend_reader.db, table, t)

	// And *not* in the writer db

	QueryDummyTableExpectMissing(d, d.sql_backend_writer.db, t)

	// Rotate again

	time.Sleep(2 * time.Second)
	d.RotateDbs()

	// Make sure its *still* in the reader (b/c static is attached)

	QueryDummyTable(d, d.sql_backend_reader.db, table, t)

	Shutdown(d)
}

func TestPersistTablePublish(t *testing.T) {

	d := GetTestDataManager(t)

	table := GetDummyTablePersist()

	// Publish table

	PublishDummyTable(d, d.sql_backend_writer.db, table, t)

	// Check its there.

	QueryDummyTable(d, d.sql_backend_writer.db, table, t)

	// Rotate the dbs

	time.Sleep(2 * time.Second)
	d.RotateDbs()

	// Check its in the reader db

	QueryDummyTable(d, d.sql_backend_reader.db, table, t)

	// And *also* in the writer db (it gets added at the end of rotate)

	QueryDummyTable(d, d.sql_backend_writer.db, table, t)

	// Rotate again

	time.Sleep(2 * time.Second)
	d.RotateDbs()

	// Make sure its *still* in the reader (b/c persist gets re-added!)

	QueryDummyTable(d, d.sql_backend_reader.db, table, t)

	Shutdown(d)
}
