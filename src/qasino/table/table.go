package table

import (
	"bytes"
	"errors"
	"fmt"

	"qasino/util"
)

type Properties struct {
	Update   bool   `json:"update,omitempty"`
	Static   bool   `json:"static,omitempty"`
	Persist  bool   `json:"persist,omitempty"`
	KeyCols  string `json:"keycols,omitempty"`
	Identity string `json:"identity,omitempty"`
}

type ColumnInfo struct {
	Name string
	Type string
}

type Table struct {
	Tablename   string          `json:"tablename"`
	Properties  Properties      `json:"properties,omitempty"`
	ColumnNames []string        `json:"column_names"`
	ColumnTypes []string        `json:"column_types"`
	ColumnDescs []string        `json:"column_descs,omitempty"`
	Rows        [][]interface{} `json:"rows"`
}

func NewTable(tablename string) *Table {
	return &Table{
		Tablename:   tablename,
		ColumnNames: make([]string, 0),
		ColumnTypes: make([]string, 0),
		ColumnDescs: make([]string, 0),
		Rows:        [][]interface{}{},
	}
}

func (t *Table) String() string {
	var buf bytes.Buffer
	buf.WriteString("Tablename: " + t.Tablename + "\nColumnNames: ")
	for _, column := range t.ColumnNames {
		buf.WriteString(" " + column)
	}
	buf.WriteString("\nColumnTypes: ")
	for _, typ := range t.ColumnTypes {
		buf.WriteString(" " + typ)
	}
	buf.WriteString("\nRows:\n")
	for _, row := range t.Rows {
		for _, cell := range row {
			buf.WriteString(fmt.Sprintf(" %v", cell))
		}
		buf.WriteString("\n")
	}
	return buf.String()
}

func (t *Table) AddColumn(column_name, typ string) {
	t.ColumnNames = append(t.ColumnNames, column_name)
	t.ColumnTypes = append(t.ColumnTypes, typ)
}

func (t *Table) AddRow(values ...interface{}) error {
	if len(values) != len(t.ColumnNames) {
		return errors.New(fmt.Sprintf("qasino table '%s' has mismatched number of columns: %d (row) != %d (column names)",
			t.Tablename, len(values), len(t.ColumnNames)))
	}
	t.Rows = append(t.Rows, values)
	return nil
}

func (t *Table) GetTablename() string {
	return t.Tablename
}

func (t *Table) SetProperty(name string, value interface{}) {
	switch name {
	case "update":
		t.Properties.Update = util.ToBoolDefault(value, false)
	case "static":
		t.Properties.Static = util.ToBoolDefault(value, false)
	case "persist":
		t.Properties.Persist = util.ToBoolDefault(value, false)
	case "keycols":
		t.Properties.KeyCols = fmt.Sprintf("%v", value)
	case "identity":
		t.Properties.Identity = fmt.Sprintf("%v", value)
	}
}

func (t *Table) GetBoolProperty(name string) bool {
	switch name {
	case "update":
		return t.Properties.Update
	case "static":
		return t.Properties.Static
	case "persist":
		return t.Properties.Persist
	}
	return false
}

func (t *Table) GetStringProperty(name string) string {
	switch name {
	case "keycols":
		return t.Properties.KeyCols
	case "identity":
		return t.Properties.Identity
	}
	return ""
}

func (t *Table) ZipColumns() []*ColumnInfo {

	if len(t.ColumnNames) != len(t.ColumnTypes) {
		return nil
	}
	ci := make([]*ColumnInfo, len(t.ColumnNames))
	for i, cn := range t.ColumnNames {
		ci[i] = &ColumnInfo{Name: cn, Type: t.ColumnTypes[i]}
	}
	return ci
}

func (t *Table) GetNumRows() int {
	return len(t.Rows)
}
