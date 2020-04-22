# Sif Parquet Parser

An Parquet DataSource Parser for Sif.

```bash
$ go get github.com/go-sif/sif-parser-parquet@master
```

**Note:** For the moment, this parser is restricted to simple, flat Parquet files, with no support for nested or repeated columns.

## Usage

```go
import (
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/schema"
	"github.com/go-sif/sif/datasource/file"
	parquet "github.com/go-sif/sif-parser-parquet"
)

// Create a `Schema` which represents the fields you intend to extract from each document in the target index. Column names should be parquet "paths", as defined by github.com/xitongsys/parquet-go (see https://github.com/xitongsys/parquet-go/blob/master/example/column_read.go for path examples).

schema := schema.CreateSchema()
schema.CreateColumn("id", &sif.Int32ColumnType{})
schema.CreateColumn("name", &sif.StringColumnType{Length: 12})
schema.CreateColumn("age", &sif.Int32ColumnType{})
schema.CreateColumn("weight", &sif.Float32ColumnType{})

// Then, connect the `Parser` to a `DataSource` which supports parsing:

parser := parquet.CreateParser(&parquet.ParserConf{
	PartitionSize: 128,
})

dataframe := file.CreateDataFrame("*.parquet", parser, schema)
```
