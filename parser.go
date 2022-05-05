package ddlmaker

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/nao1215/ddl-maker/dialect"
	"github.com/nao1215/nameconv"
)

// Table is for type assertion
type Table interface {
	Table() string
}

// PrimaryKey is for type assertion
type PrimaryKey interface {
	PrimaryKey() dialect.PrimaryKey
}

// ForeignKey is for type assertion
type ForeignKey interface {
	ForeignKeys() dialect.ForeignKeys
}

// Index is for type assertion
type Index interface {
	Indexes() dialect.Indexes
}

func (dm *DDLMaker) parse() error {
	for _, s := range dm.Structs {
		val := reflect.Indirect(reflect.ValueOf(s))
		rt := val.Type()

		var columns []dialect.Column
		for i := 0; i < rt.NumField(); i++ {
			rtField := rt.Field(i)
			column, err := parseField(rtField, dm.Dialect)
			if err != nil {
				if err == ErrIgnoreField {
					continue
				}
				return fmt.Errorf("error parse field: %w", err) // This pass will not go through.
			}
			columns = append(columns, column)
		}

		table := parseTable(s, columns, dm.Dialect)
		dm.Tables = append(dm.Tables, table)
	}
	return nil
}

func parseField(field reflect.StructField, d dialect.Dialect) (dialect.Column, error) {
	tagStr := strings.Replace(field.Tag.Get(TAGPREFIX), " ", "", -1)

	for _, tag := range strings.Split(tagStr, ",") {
		if tag == IGNORETAG {
			return nil, ErrIgnoreField
		}
	}

	var typeName string
	switch {
	case field.Type.PkgPath() != "":
		// ex) time.Time
		pkgName := field.Type.PkgPath()
		if strings.Contains(pkgName, "/") {
			pkgs := strings.Split(pkgName, "/")
			pkgName = pkgs[len(pkgs)-1]
		}
		typeName = fmt.Sprintf("%s.%s", pkgName, field.Type.Name())
	case field.Type.Kind() == reflect.Ptr:
		// pointer type
		typeName = fmt.Sprintf("*%s", field.Type.Elem())
	case field.Type.Kind() == reflect.Slice:
		// slice type
		typeName = fmt.Sprintf("[]%s", field.Type.Elem())
	default:
		typeName = field.Type.Name()
	}

	return newColumn(nameconv.ToSnakeCase(field.Name), typeName, tagStr, d), nil
}

func parseTable(s interface{}, columns []dialect.Column, d dialect.Dialect) dialect.Table {
	var tableName string
	var primaryKey dialect.PrimaryKey
	var foreignKeys dialect.ForeignKeys
	var indexes dialect.Indexes

	if v, ok := s.(Table); ok {
		tableName = nameconv.ToSnakeCase(v.Table())
	} else {
		val := reflect.Indirect(reflect.ValueOf(s))
		tableName = nameconv.ToSnakeCase(val.Type().Name())
	}
	if v, ok := s.(PrimaryKey); ok {
		primaryKey = v.PrimaryKey()
	}
	if v, ok := s.(ForeignKey); ok {
		foreignKeys = v.ForeignKeys()
	}
	if v, ok := s.(Index); ok {
		indexes = v.Indexes()
	}

	return newTable(tableName, primaryKey, foreignKeys, columns, indexes, d)
}
