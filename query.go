package efgo

import (
	"context"
	"database/sql"
	"reflect"
)

func QueryRow[T any](db *sql.DB, query string, args ...interface{}) (*T, error) {
	return QueryRowContext[T](context.Background(), db, query, args...)
}

func QueryRowContext[T any](ctx context.Context, db *sql.DB, query string, args ...interface{}) (*T, error) {
	if reflect.TypeFor[T]().Kind() != reflect.Struct {
		panic("can not query row context of non-struct type")
	}

	result := new(T)
	val := reflect.ValueOf(result).Elem()
	typ := val.Type()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	scanArgs := make([]reflect.Value, len(cols))
	for i, col := range cols {
		for j := 0; j < typ.NumField(); j++ {
			f := val.Field(j).Addr()
			ft := typ.Field(j)

			if col == ft.Tag.Get("db") {
				scanArgs[i] = f
			}
		}
	}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}

	if err := reflect.ValueOf(rows.Scan).Call(scanArgs)[0]; !err.IsNil() {
		return nil, err.Interface().(error)
	}

	return result, nil
}

func Query[T any](db *sql.DB, query string, args ...any) ([]*T, error) {
	return QueryContext[T](context.Background(), db, query, args...)
}

func QueryContext[T any](ctx context.Context, db *sql.DB, query string, args ...any) ([]*T, error) {
	if reflect.TypeFor[T]().Kind() != reflect.Struct {
		panic("can not query row context of non-struct type")
	}

	typ := reflect.TypeFor[T]()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	fieldIndexes := make([][]int, len(cols))
	for i, col := range cols {
		for j := 0; j < typ.NumField(); j++ {
			ft := typ.Field(j)

			if col == ft.Tag.Get("db") {
				fieldIndexes[i] = ft.Index
			}
		}
	}

	result := reflect.MakeSlice(reflect.SliceOf(reflect.PointerTo(typ)), 0, 0)
	scanValue := reflect.ValueOf(rows.Scan)

	for rows.Next() {
		valAddr := reflect.New(typ)
		val := valAddr.Elem()

		scanArgs := make([]reflect.Value, len(cols))
		for i := range cols {
			f := val.FieldByIndex(fieldIndexes[i]).Addr()
			scanArgs[i] = f
		}

		if err := scanValue.Call(scanArgs)[0]; !err.IsNil() {
			return nil, err.Interface().(error)
		}

		result = reflect.Append(result, valAddr)
	}

	return result.Interface().([]*T), nil
}
