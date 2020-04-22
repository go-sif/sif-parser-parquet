package parquet

import (
	"fmt"
	"time"

	"github.com/go-sif/sif"
)

func parseValue(colName string, colType sif.ColumnType, val interface{}, row sif.Row) error {
	// TODO nil handling
	if val == nil {
		row.SetNil(colName)
		return nil
	}
	// parse type
	switch colType.(type) {
	// TODO array/slice type
	case *sif.BoolColumnType:
		row.SetBool(colName, val.(bool))
	case *sif.Int8ColumnType:
		row.SetInt8(colName, val.(int8))
	case *sif.Int16ColumnType:
		row.SetInt16(colName, val.(int16))
	case *sif.Int32ColumnType:
		row.SetInt32(colName, val.(int32))
	case *sif.Int64ColumnType:
		row.SetInt64(colName, val.(int64))
	case *sif.Float32ColumnType:
		row.SetFloat32(colName, val.(float32))
	case *sif.Float64ColumnType:
		row.SetFloat64(colName, val.(float64))
	case *sif.StringColumnType:
		row.SetString(colName, val.(string))
	case *sif.TimeColumnType:
		format := colType.(*sif.TimeColumnType).Format
		tval, err := time.Parse(format, val.(string))
		if err != nil {
			return fmt.Errorf("Column %s could not be parsed as datetime with format %s. Was: %#v", colName, format, val)
		}
		row.SetTime(colName, tval)
	case *sif.VarStringColumnType:
		row.SetVarString(colName, val.(string))
	default:
		return fmt.Errorf("Parquet parsing does not support column type %T", colType)
	}
	return nil
}
