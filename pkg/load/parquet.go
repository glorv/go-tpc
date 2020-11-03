package load

import (
	"github.com/xitongsys/parquet-go/writer"
	"io"
)

func NewParquetWriter(w io.Writer, obj interface{}) (*writer.ParquetWriter, error) {
	return writer.NewParquetWriterFromWriter(w, obj, 4)
}
