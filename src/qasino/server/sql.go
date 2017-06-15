package server

import (
	"database/sql"
	"sync"
	"time"

	"qasino/util"

	"github.com/golang/glog"
)

// This just mirrors the interface for sql.DB We're basically
// expecting every backend to adhere to the database/sql api for
// better or for worse...
type SqlReaderWriter interface {
	// database/sql
	Begin() (*sql.Tx, error)
	Close() error
	Exec(query string, args ...interface{}) (sql.Result, error)
	Ping() error
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats

	// Ours
	Open(generation_number int64) error
	Use()
	Unuse()
}

type SqlBackend struct {
	id       int64
	db       SqlReaderWriter
	db_mutex sync.RWMutex
	factory  func(string) SqlReaderWriter
}

type dbFunc func(SqlReaderWriter)

// This is a convienence for operations on the database.  Increment a
// use count before and decrement after - the decrement will close if
// it goes to zero.  'db' might get swapped so take a ref to it under lock before using.
func (s *SqlBackend) Use(f dbFunc) {
	glog.V(2).Infof("SqlBackend: Use %d\n", s.id)
	s.db.Use()
	s.db_mutex.RLock()
	db := s.db
	s.db_mutex.RUnlock()
	f(db)
	s.db.Unuse()
	glog.V(2).Infof("SqlBackend: Unuse %d\n", s.id)
}

func NewSqlBackend(filename string) *SqlBackend {

	switch util.ConfGetStringDefault("backend.type", "sqlite") {
	case "sqlite":
		id := time.Now().Unix()
		sqlite_backend := NewSqliteBackend(filename)
		return &SqlBackend{
			id:      id,
			db:      sqlite_backend,
			factory: NewSqliteBackend,
		}
	}
	return nil
}
