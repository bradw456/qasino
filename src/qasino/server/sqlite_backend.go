package server

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/golang/glog"
	_ "github.com/mattn/go-sqlite3"
)

type UseCountCloser struct {
	mutex      sync.RWMutex
	count      int
	close_func func() error
	name       string
}

func NewUseCountCloser(f func() error, name string) *UseCountCloser {
	return &UseCountCloser{
		mutex:      sync.RWMutex{},
		count:      0,
		close_func: f,
		name:       name,
	}
}

// Increment the use counter.
func (u *UseCountCloser) Inc() {
	u.mutex.Lock()
	u.count++
	glog.V(2).Infof("SqliteBackend: Use() %d\n", u.count)
	u.mutex.Unlock()
}

// Decrement the use count and call the close func if it goes to zero.
func (u *UseCountCloser) Dec() {
	close := false
	u.mutex.Lock()
	u.count--
	glog.V(2).Infof("SqliteBackend: Unuse() %d\n", u.count)
	if u.count == 0 {
		close = true
	}
	u.mutex.Unlock()
	if close {
		err := u.close_func()
		if err != nil {
			glog.Errorf("Error closing '%s': %s\n", u.name, err)
		}
	}
}

type SqliteBackend struct {
	sql.DB
	filename    string
	is_template bool
	use_count   *UseCountCloser
}

func NewSqliteBackend(filename string) SqlReaderWriter {

	s := &SqliteBackend{
		filename: filename,
	}
	if strings.Contains(filename, "%d") {
		s.is_template = true
	}
	return SqlReaderWriter(s)
}

func (s *SqliteBackend) Open(generation_number int64) error {

	// TODO set max idle conn to ConfGetIntDefault("backend.pool_size", 10),

	var err error
	var filename string

	if s.is_template {
		filename = fmt.Sprintf(s.filename, generation_number)
	} else {
		filename = s.filename
	}

	glog.V(1).Infof("Opening %s\n", filename)

	new_db, err := sql.Open("sqlite3", filename)
	if err != nil {
		errorstr := fmt.Sprintf("Error opening sqlite '%s': %s\n", filename, err)
		glog.Errorln(errorstr)
		return err
	}

	s.use_count = NewUseCountCloser(func() error {
		if filename == ":memory:" {
			glog.V(1).Infof("Closing generation %d\n", generation_number)
			err := new_db.Close()
			if err != nil {
				return err
			}
			return nil
		} else {
			glog.V(1).Infof("Closing and deleting generation %d: %s\n", generation_number, filename)
			err := new_db.Close()
			if err != nil {
				return err
			}
			return os.Remove(filename)
		}
	}, filename)

	s.DB = *new_db

	return nil
}

func (s *SqliteBackend) Use() {
	s.use_count.Inc()
}

func (s *SqliteBackend) Unuse() {
	s.use_count.Dec()
}
