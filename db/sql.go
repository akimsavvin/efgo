package db

import (
	"context"
	"errors"
	"log"
	"reflect"
)

var (
	ErrMultipleRecords = errors.New("multiple records found")
	ErrNoRecords       = errors.New("no records found")
)

// checkDB returns nil or ErrNotInitialized if db was not initialized
func checkDB() {
	if db == nil {
		log.Panicln("database was not initialized")
	}
}

// Query executes a query and returns satisfying elements.
func Query[T any](ctx context.Context, sql string, args ...any) ([]*T, error) {
	checkDB()

	rows, err := db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]*T, 0)

	rowsVal := reflect.ValueOf(rows)

	colsVals := rowsVal.MethodByName("Columns").Call(make([]reflect.Value, 0))
	if !colsVals[1].IsNil() {
		return nil, colsVals[1].Interface().(error)
	}

	cols := colsVals[0].Interface().([]string)

	zeroVal := reflect.Value{}
	empty := reflect.ValueOf(new(struct {
		Empty string
	})).Elem().Field(0).Addr()

	for rows.Next() {
		rowPtrVal := reflect.ValueOf(new(T))
		rowVal := rowPtrVal.Elem()
		rowTyp := rowVal.Type()

		colVals := make([]reflect.Value, len(cols))

		for i, col := range cols {
			for j := range rowVal.NumField() {
				sf := rowTyp.Field(j)

				colName := sf.Tag.Get("db")
				if colName == col {
					colVals[i] = rowVal.Field(j).Addr()
				}
			}

			if colVals[i] == zeroVal {
				colVals[i] = empty
			}
		}

		scanVals := rowsVal.MethodByName("Scan").Call(colVals)
		if !scanVals[0].IsNil() {
			return nil, scanVals[0].Interface().(error)
		}

		res = append(res, rowPtrVal.Interface().(*T))
	}

	return res, nil
}

// QueryFirst executes a single-row query and returns the first element.
func QueryFirst[T any](ctx context.Context, sql string, args ...any) (*T, error) {
	sql += " LIMIT 1"
	res, err := Query[T](ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, ErrNoRecords
	}

	return res[0], nil
}

// QuerySingle executes a single-row query and returns the first element or ErrMultipleRecords if more than one is found.
func QuerySingle[T any](ctx context.Context, sql string, args ...any) (*T, error) {
	res, err := Query[T](ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	if len(res) > 1 {
		return nil, ErrMultipleRecords
	}

	if len(res) == 0 {
		return nil, ErrNoRecords
	}

	return res[0], nil
}

// Execute executes provided SQL statement and returns number of affected rows.
func Execute(ctx context.Context, sql string, args ...any) (int, error) {
	checkDB()

	res, err := db.ExecContext(ctx, sql, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(rowsAffected), nil
}
