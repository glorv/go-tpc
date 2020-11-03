package load

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"os"
)

const (
	maxBatchCount = 1024
)

// SQLBatchLoader helps us insert in batch
type SQLBatchLoader struct {
	insertHint string
	conn       *sql.Conn
	buf        bytes.Buffer
	count      int
}

// NewSQLBatchLoader creates a batch loader for database connection
func NewSQLBatchLoader(conn *sql.Conn, hint string) *SQLBatchLoader {
	return &SQLBatchLoader{
		count:      0,
		insertHint: hint,
		conn:       conn,
	}
}

// InsertValue inserts a value, the loader may flush all pending values.
func (b *SQLBatchLoader) InsertValue(ctx context.Context, query []string) error {
	sep := ", "
	if b.count == 0 {
		b.buf.WriteString(b.insertHint)
		sep = " "
	}
	b.buf.WriteString(sep)
	b.buf.WriteString(query[0])

	b.count++

	if b.count >= maxBatchCount {
		return b.Flush(ctx)
	}

	return nil
}

// Flush inserts all pending values
func (b *SQLBatchLoader) Flush(ctx context.Context) error {
	if b.buf.Len() == 0 {
		return nil
	}

	_, err := b.conn.ExecContext(ctx, b.buf.String())
	b.count = 0
	b.buf.Reset()

	return err
}

type BatchLoader interface {
	Name() string
	InsertValue(ctx context.Context, columns interface{}) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}

// CSVBatchLoader helps us insert in batch
type CSVBatchLoader struct {
	f      *os.File
	writer *csv.Writer
}

// NewCSVBatchLoader creates a batch loader for csv format
func NewCSVBatchLoader(f *os.File) *CSVBatchLoader {
	return &CSVBatchLoader{
		f:      f,
		writer: csv.NewWriter(f),
	}
}

func (b *CSVBatchLoader) Name() string {
	return "csv"
}

// InsertValue inserts a value, the loader may flush all pending values.
func (b *CSVBatchLoader) InsertValue(ctx context.Context, columns interface{}) error {
	return b.writer.Write(columns.([]string))
}

// Flush inserts all pending values
func (b *CSVBatchLoader) Flush(ctx context.Context) error {
	b.writer.Flush()
	return b.writer.Error()
}

// Close closes the file.
func (b *CSVBatchLoader) Close(ctx context.Context) error {
	return b.f.Close()
}

type ParquetBatchLoader struct {
	f source.ParquetFile
	w *writer.ParquetWriter
	name string
	closed bool
}

func NewParquetBatchLoader(path string, o interface{}, name string) (BatchLoader, error) {
	f, err := local.NewLocalFileWriter(path)
	w, err :=  writer.NewParquetWriter(f, o, 4)
	if err != nil {
		return nil, err
	}

	return &ParquetBatchLoader{
		f: f,
		w:w,
		name: name,
	}, nil
}

func (b *ParquetBatchLoader) Name() string {
	return fmt.Sprintf("paquet-%s",b.name)
}

// InsertValue inserts a value, the loader may flush all pending values.
func (b *ParquetBatchLoader) InsertValue(ctx context.Context, columns interface{}) error {
	return b.w.Write(columns)
}

// Flush inserts all pending values
func (b *ParquetBatchLoader) Flush(ctx context.Context) error {
	err := b.w.Flush(false)
	return err
}

// Close closes the file.
func (b *ParquetBatchLoader) Close(ctx context.Context) error {
	if b.closed {
		return nil
	}

	err := b.w.WriteStop()
	if err != nil {
		return err
	}
	b.closed = true
	return b.f.Close()
}