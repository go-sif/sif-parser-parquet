package parquet

import (
	"fmt"
	"sync"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/datasource"
	"github.com/go-sif/sif/errors"

	"github.com/xitongsys/parquet-go/reader"
)

type partitionIterator struct {
	parser              *Parser
	parquetReader       *reader.ParquetReader
	finished            bool
	source              sif.DataSource
	schema              sif.Schema
	widestInitialSchema sif.Schema
	lock                sync.Mutex
	endListeners        []func()
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (pi *partitionIterator) OnEnd(onEnd func()) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	pi.endListeners = append(pi.endListeners, onEnd)
}

// HasNextPartition returns true iff this PartitionIterator can produce another Partition
func (pi *partitionIterator) HasNextPartition() bool {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	return !pi.finished
}

// NextPartition returns the next Partition if one is available, or an error
func (pi *partitionIterator) NextPartition() (sif.Partition, error) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	numRows := pi.parquetReader.GetNumRows()
	if int64(pi.parser.PartitionSize()) < numRows {
		numRows = int64(pi.parser.PartitionSize())
	}
	colTypes := pi.schema.ColumnTypes()
	colNames := pi.schema.ColumnNames()
	part := datasource.CreateBuildablePartition(pi.parser.PartitionSize(), pi.widestInitialSchema, pi.schema)

	// Parse data one *column* at a time, since parquet is column-focused
	// https://parquet.apache.org/documentation/latest/
	// https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
	for i, name := range colNames {
		colType := colTypes[i]
		vals, repLevels, defLevels, err := pi.parquetReader.ReadColumnByPath(name, numRows)
		if err != nil {
			schemaElements := ""
			for _, se := range pi.parquetReader.SchemaHandler.IndexMap {
				schemaElements += fmt.Sprintf(" - %s\n", se)
			}
			return nil, fmt.Errorf("Unable to read column %s: %e\nSchema: \n%s", name, err, schemaElements)
		}
		if int64(len(vals)) < numRows {
			if !pi.finished {
				pi.done()
			}
		}
		if len(vals) == 0 {
			return nil, errors.NoMorePartitionsError{}
		}
		// set values row by row
		rowsInFile := numRows
		if int64(len(vals)) < rowsInFile {
			rowsInFile = int64(len(vals))
		}
		for j := 0; int64(j) < rowsInFile; j++ {
			// create empty rows for data if they don't already exist
			if j >= part.GetNumRows() {
				part.AppendEmptyRowData()
			}
			// TODO support nested values
			// TODO support repeated values
			if repLevels[j] != 0 || defLevels[j] > 1 {
				return nil, fmt.Errorf("Sif Parquet parser does not currently support repeated columns")
			}
			err = parseValue(name, colType, vals[j], part.GetRow(j))
		}
	}

	// if we fetched fewer than the partition size, then there's no more rows left
	if numRows < int64(pi.parser.PartitionSize()) {
		pi.done()
	}
	return part, nil
}

func (pi *partitionIterator) done() {
	pi.finished = true
	for _, l := range pi.endListeners {
		l()
	}
	pi.endListeners = []func(){}
}
