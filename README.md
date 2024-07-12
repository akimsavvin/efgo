## Getting started

### Install

```sh
go get https://github.com/akimsavvin/efgo
```

### Configure

```go
import (
    "driver"
)

db.Configue("driver name", "connection string")
```

### Query

```go
type User struct {
    Id int `db:"id"`
    Name string `db:"name"`
}

users, err := db.QueryFirst[User]("SELECT id, name FROM users")
if err != nil {
    panic(err)
}
```
