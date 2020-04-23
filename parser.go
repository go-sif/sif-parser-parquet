package parquet

import (
	"bytes"
	"io"

	"github.com/go-sif/sif"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

// ParserConf configures a Parquet Parser, suitable for JSON lines data
type ParserConf struct {
	PartitionSize int // The maximum number of rows per Partition. Defaults to 128.
}

// Parser produces partitions from Parquet data
type Parser struct {
	conf *ParserConf
}

// CreateParser returns a new Parquet Parser. Columns are parsed lazily from each row of JSON using their column name, which should be a gjson path. Values within the JSON which do not correspond to a Schema column are ignored.
func CreateParser(conf *ParserConf) *Parser {
	if conf.PartitionSize == 0 {
		conf.PartitionSize = 128
	}
	return &Parser{conf: conf}
}

// PartitionSize returns the maximum size in rows of Partitions produced by this Parser
func (p *Parser) PartitionSize() int {
	return p.conf.PartitionSize
}

// Parse parses Parquet data to produce Partitions
func (p *Parser) Parse(r io.Reader, source sif.DataSource, schema sif.Schema, widestInitialSchema sif.Schema, onIteratorEnd func()) (sif.PartitionIterator, error) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	parquetFile, err := buffer.NewBufferFile(buf.Bytes())
	if err != nil {
		return nil, err
	}
	reader, err := reader.NewParquetColumnReader(parquetFile, int64(p.PartitionSize()))

	iterator := &partitionIterator{
		parser:              p,
		parquetReader:       reader,
		source:              source,
		schema:              schema,
		widestInitialSchema: widestInitialSchema,
		endListeners:        []func(){},
	}
	if onIteratorEnd != nil {
		iterator.OnEnd(onIteratorEnd)
	}
	return iterator, nil
}
