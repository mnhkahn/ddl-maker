package ddlmaker

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/bournex/ordered_container"
	"github.com/mnhkahn/ddl-maker/dialect"
	"github.com/mnhkahn/ddl-maker/dialect/mysql"
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

func (dm *DDLMaker) parseJSON(data string) error {
	m := ordered_container.OrderedMap{}
	err := json.Unmarshal([]byte(data), &m)
	if err != nil {
		return err
	}
	cols := make([]dialect.Column, 0, len(m.Values))
	keyMap := make(map[string]struct{}, len(cols))
	idxs := dialect.Indexes{}
	for _, values := range m.Values {
		k := values.Key
		v := values.Value
		typeName := typeForValue(v, true)
		keyMap[k] = struct{}{}
		if typeName == "" {
			continue
		}

		col := newColumn(nameconv.ToSnakeCase(k), typeName, "", dm.Dialect)
		cols = append(cols, col)

		if strings.HasSuffix(k, "code") || strings.HasSuffix(k, "no") {
			idxs = append(idxs, mysql.AddUniqueIndex("uniq_"+k, k))
		}
	}
	// id 放最前面
	if _, ok := keyMap["id"]; !ok {
		cols = append([]dialect.Column{newColumn("id", "uint64", "auto,comment=pk", dm.Dialect)}, cols...)
	}
	if _, ok := keyMap["is_deleted"]; !ok {
		cols = append(cols, newColumn("is_deleted", "uint8", "default=0,comment=0 valid/1 deleted", dm.Dialect))
	}
	if _, ok := keyMap["create_time"]; !ok {
		cols = append(cols, newColumn("create_time", "time.Time", "default=CURRENT_TIMESTAMP", dm.Dialect))
	}
	if _, ok := keyMap["update_time"]; !ok {
		cols = append(cols, newColumn("update_time", "time.Time", "default=CURRENT_TIMESTAMP,update=CURRENT_TIMESTAMP", dm.Dialect))
	}
	idxs = append(idxs, mysql.AddIndex("idx_create_time", "create_time"))

	table := newTable("foo", mysql.AddPrimaryKey("id"), nil, cols, idxs, dm.Dialect)
	dm.Tables = append(dm.Tables, table)

	return nil
}

func typeForValue(value interface{}, convertFloats bool) string {
	v := reflect.TypeOf(value).Name()
	if v == "float64" && convertFloats {
		v = disambiguateFloatInt(value)
	}
	return v
}

func disambiguateFloatInt(value interface{}) string {
	const epsilon = .0001
	vfloat := value.(float64)
	if math.Abs(vfloat-math.Floor(vfloat+epsilon)) < epsilon {
		var tmp int64
		return reflect.TypeOf(tmp).Name()
	}
	return reflect.TypeOf(value).Name()
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
