package efgo

import (
	"context"
	"database/sql"
	"reflect"
)

type QueryExec interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func QueryRow[T any](qx QueryExec, query string, args ...interface{}) (*T, error) {
	return QueryRowContext[T](context.Background(), qx, query, args...)
}

func QueryRowContext[T any](ctx context.Context, qx QueryExec, query string, args ...interface{}) (*T, error) {
	if reflect.TypeFor[T]().Kind() != reflect.Struct {
		panic("can not query row context of non-struct type")
	}

	result := new(T)
	val := reflect.ValueOf(result).Elem()
	typ := val.Type()

	rows, err := qx.QueryContext(ctx, query, args...)
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

			if col == ft.Tag.Get("qx") {
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

func Query[T any](qx QueryExec, query string, args ...any) ([]*T, error) {
	return QueryContext[T](context.Background(), qx, query, args...)
}

func QueryContext[T any](ctx context.Context, qx QueryExec, query string, args ...any) ([]*T, error) {
	if reflect.TypeFor[T]().Kind() != reflect.Struct {
		panic("can not query row context of non-struct type")
	}

	typ := reflect.TypeFor[T]()

	rows, err := qx.QueryContext(ctx, query, args...)
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
