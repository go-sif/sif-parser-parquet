package test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/go-sif/sif"
	parquet "github.com/go-sif/sif-parser-parquet"
	"github.com/go-sif/sif/datasource/file"
	"github.com/go-sif/sif/errors"
	"github.com/go-sif/sif/schema"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type Record struct {
	ID     int32   `parquet:"name=id, type=INT32"`
	Name   string  `parquet:"name=name, type=UTF8"`
	Age    *int32  `parquet:"name=age, type=INT32"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
}

func writeTestFiles() {
	var err error
	//write
	fw, err := local.NewLocalFileWriter("test.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err :=
		writer.NewParquetWriter(fw, new(Record), 4)
	if err != nil {
		log.Println("Can't create parquet writer")
		return
	}
	num := 1000
	for i := 0; i < num; i++ {
		var age *int32
		if i%2 == 0 {
			k := int32(20 + i%5)
			age = &k
		}
		stu := Record{
			ID:     int32(i),
			Name:   fmt.Sprintf("record-%d", i),
			Age:    age,
			Weight: float32(50.0 + float32(i)*0.1),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	log.Println("Write Finished")
	fw.Close()
}

func removeTestFiles() {
	os.Remove("test.parquet")
}

func TestParquetParser(t *testing.T) {
	writeTestFiles()
	defer removeTestFiles()

	parser := parquet.CreateParser(&parquet.ParserConf{
		PartitionSize: 128,
	})
	schema := schema.CreateSchema()
	schema.CreateColumn("parquet_go_root.id", &sif.Int32ColumnType{})
	schema.CreateColumn("parquet_go_root.name", &sif.StringColumnType{Length: 12})
	schema.CreateColumn("parquet_go_root.age", &sif.Int32ColumnType{})
	schema.CreateColumn("parquet_go_root.weight", &sif.Float32ColumnType{})
	conf := &file.DataSourceConf{Glob: "test.parquet"}
	dataframe := file.CreateDataFrame(conf, parser, schema)

	pm, err := dataframe.GetDataSource().Analyze()
	require.Nil(t, err, "Analyze err should be null")
	totalPartitions := 0
	totalRows := 0
	for pm.HasNext() {
		pl := pm.Next()
		ps, err := pl.Load(parser, schema)
		require.Nil(t, err)
		for ps.HasNextPartition() {
			part, err := ps.NextPartition()
			if _, ok := err.(errors.NoMorePartitionsError); !ok {
				require.Nil(t, err)
			}
			totalPartitions++
			totalRows += part.GetNumRows()
		}
	}
	require.False(t, pm.HasNext())
	require.Equal(t, 1000, totalRows)
}
