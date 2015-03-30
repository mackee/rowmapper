package mapper

import (
	"database/sql"
	"errors"
	"reflect"
)

type Mapper struct {
	Rows    *sql.Rows
	columns []string
}

func NewMapper(rows *sql.Rows) (*Mapper, error) {
	columns, err := rows.Columns()
	if err != nil {
		return &Mapper{}, err
	}

	m := &Mapper{Rows: rows, columns: columns}
	return m, nil
}

func (m *Mapper) Next(row interface{}) (ok bool, err error) {
	defer func() {
		errMsg := recover()
		if errMsg != nil {
			err = errors.New(errMsg.(string))
		}
	}()
	ok = m.next(row)

	return ok, nil
}

func (m *Mapper) next(row interface{}) bool {
	rowType := reflect.TypeOf(row).Elem()
	columnFieldMap := make(map[string]string)
	for i := 0; i < rowType.NumField(); i++ {
		field := rowType.Field(i)
		column := field.Tag.Get("db")
		if column != "" && column != "-" {
			columnFieldMap[column] = field.Name
		}
	}

	rowValue := reflect.ValueOf(row)

	columns := m.columns
	columnValueMap := make(map[string]interface{})
	columnValues := make([]interface{}, 0)
	for _, column := range columns {
		fieldName, ok := columnFieldMap[column]
		if !ok {
			columnValues = append(columnValues, struct{}{})
		}

		fieldValue := rowValue.Elem().FieldByName(fieldName)
		switch fieldValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			var value int64
			columnValueMap[fieldName] = &value
			columnValues = append(columnValues, &value)
		case reflect.String:
			var value string
			columnValueMap[fieldName] = &value
			columnValues = append(columnValues, &value)
		case reflect.Float32, reflect.Float64:
			var value float64
			columnValueMap[fieldName] = &value
			columnValues = append(columnValues, &value)
		case reflect.Bool:
			var value bool
			columnValueMap[fieldName] = &value
			columnValues = append(columnValues, &value)
		}
	}

	ok := m.Rows.Next()
	if !ok {
		return false
	}
	m.Rows.Scan(columnValues...)

	for field, value := range columnValueMap {
		fieldValue := rowValue.Elem().FieldByName(field)
		switch value.(type) {
		case *int64:
			fieldValue.SetInt(*value.(*int64))
		case *string:
			fieldValue.SetString(*value.(*string))
		case *float64:
			fieldValue.SetFloat(*value.(*float64))
		case *bool:
			fieldValue.SetBool(*value.(*bool))
		}
	}

	return true
}
