package parquet

import (
	"fmt"
	"sync"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/datasource"
	"github.com/go-sif/sif/errors"

	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

type partitionIterator struct {
	parser                     *Parser
	parquetFile                source.ParquetFile
	readerMapLock              sync.Mutex
	readers                    map[string]*reader.ParquetReader
	readerLocks                map[string]*sync.Mutex
	finished                   bool
	source                     sif.DataSource
	schema                     sif.Schema
	widestInitialPrivateSchema sif.Schema
	lock                       sync.Mutex
	doneLock                   sync.Mutex
	endListeners               []func()
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
	colTypes := pi.schema.ColumnTypes()
	colNames := pi.schema.ColumnNames()
	part := datasource.CreateBuildablePartition(pi.parser.PartitionSize(), pi.widestInitialPrivateSchema, pi.schema)

	// Parse data one *column* at a time, since parquet is column-focused
	// https://parquet.apache.org/documentation/latest/
	// https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
	// TODO parallel column loading?

	var partitionRowAddLock sync.Mutex
	var wg sync.WaitGroup
	rowLocks := make([]sync.Mutex, pi.parser.PartitionSize())
	errorChan := make(chan error)
	wgDone := make(chan bool)
	for i, name := range colNames {
		pi.readerMapLock.Lock()
		if pi.readers[name] == nil {
			reader, err := reader.NewParquetColumnReader(pi.parquetFile, int64(pi.parser.PartitionSize()))
			if err != nil {
				return nil, err
			}
			pi.readerLocks[name] = &sync.Mutex{}
			pi.readers[name] = reader
		}
		pi.readerMapLock.Unlock()
		wg.Add(1)
		go func(idx int, name string) {
			defer wg.Done()
			pi.readerLocks[name].Lock()
			numRows := pi.readers[name].GetNumRows()
			pi.readerLocks[name].Unlock()
			if int64(pi.parser.PartitionSize()) < numRows {
				numRows = int64(pi.parser.PartitionSize())
			}
			colType := colTypes[idx]
			pi.readerLocks[name].Lock()
			vals, repLevels, defLevels, err := pi.readers[name].ReadColumnByPath(name, numRows)
			pi.readerLocks[name].Unlock()
			if err != nil {
				schemaElements := ""
				for _, se := range pi.readers[name].SchemaHandler.IndexMap {
					schemaElements += fmt.Sprintf(" - %s\n", se)
				}
				errorChan <- fmt.Errorf("Unable to read column %s: %e\nSchema: \n%s", name, err, schemaElements)
				return
			}
			if int64(len(vals)) < numRows {
				if !pi.finished {
					pi.done()
				}
			}
			if len(vals) == 0 {
				errorChan <- errors.NoMorePartitionsError{}
				return
			}
			// set values row by row
			rowsInFile := numRows
			if int64(len(vals)) < rowsInFile {
				rowsInFile = int64(len(vals))
			}
			tempRow := datasource.CreateTempRow()
			for j := 0; int64(j) < rowsInFile; j++ {
				// create empty rows for data if they don't already exist
				partitionRowAddLock.Lock()
				var row sif.Row
				if j >= part.GetNumRows() {
					row, err = part.AppendEmptyRowData(tempRow)
					if err != nil {
						errorChan <- fmt.Errorf("Unable to append empty row to partition")
						return
					}
				} else {
					row = part.GetRow(j)
				}
				partitionRowAddLock.Unlock()
				// TODO support nested values
				// TODO support repeated values
				if repLevels[j] != 0 || defLevels[j] > 1 {
					errorChan <- fmt.Errorf("Sif Parquet parser does not currently support repeated columns")
					return
				}
				// lock row if the column is variable-length, since var columns use maps
				if sif.IsVariableLength(colType) {
					rowLocks[j].Lock()
				}
				err = parseValue(name, colType, vals[j], row)
				if err != nil {
					errorChan <- err
					return
				}
				if sif.IsVariableLength(colType) {
					rowLocks[j].Unlock()
				}
			}
		}(i, name)
	}
	// wait for all columns to be loaded into the partition
	go func() {
		wg.Wait()
		close(wgDone)
	}()

	// check for errors
	select {
	case <-wgDone:
		// we're done!
		break
	case err := <-errorChan:
		// close(errorChan) other routines might need to write errors
		return nil, err
	}

	// if we fetched fewer than the partition size, then there's no more rows left
	if part.GetNumRows() < pi.parser.PartitionSize() {
		pi.done()
	}
	return part, nil
}

func (pi *partitionIterator) done() {
	pi.doneLock.Lock()
	defer pi.doneLock.Unlock()
	pi.finished = true
	for _, l := range pi.endListeners {
		l()
	}
	pi.endListeners = []func(){}
}
