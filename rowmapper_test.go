package mapper

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type Table1 struct {
	Id          int    `db:"id"`
	Name        string `db:"name"`
	Description string `db:"-"`
}

func initTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal("db connection open error:", err)
	}

	_, err = db.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal("create table error:", err)
	}
	return db
}

func TestMapperNext(t *testing.T) {
	db := initTestDB(t)

	_, err := db.Exec("INSERT INTO t1 VALUES(?, ?)", 42, "hogehoge")
	if err != nil {
		t.Fatal("insert fixture data error:", err)
	}

	rows, err := db.Query("SELECT * FROM t1 WHERE id = ?", 42)
	if err != nil {
		t.Fatal("select error:", err)
	}

	mapper, err := NewMapper(rows)
	if err != nil {
		t.Fatal("new mapper error:", err)
	}

	t1 := new(Table1)
	ok, err := mapper.Next(t1)
	if !ok {
		t.Error("mapper Next() is not ok")
	}
	if err != nil {
		t.Fatal("mapper Next() error:", err)
	}
	if t1.Id != 42 {
		t.Error("t1.Id is not 42:", t1.Id)
	}
	if t1.Name != "hogehoge" {
		t.Error("t1.Name is not hogehoge:", t1.Name)
	}
	if t1.Description != "" {
		t.Error("t1.Description is in unexpected value:", t1.Description)
	}
}

func TestMapperNextMulti(t *testing.T) {
	db := initTestDB(t)

	_, err := db.Exec(`
	INSERT INTO t1
	SELECT 1, "hokkaido"
	UNION ALL SELECT 2, "aomori"
	UNION ALL SELECT 3, "iwate"
	UNION ALL SELECT 4, "miyagi"
	UNION ALL SELECT 5, "akita";
	`)
	if err != nil {
		t.Fatal("insert fixture data error:", err)
	}

	expectedMap := map[int]string{
		1: "hokkaido",
		2: "aomori",
		3: "iwate",
		4: "miyagi",
		5: "akita",
	}

	rows, err := db.Query("SELECT * FROM t1")
	if err != nil {
		t.Fatal("select all error:", err)
	}

	mapper, err := NewMapper(rows)
	if err != nil {
		t.Error("new mapper error:", err)
	}

	for i := 1; i <= 5; i++ {
		t1 := new(Table1)
		ok, err := mapper.Next(t1)
		if !ok {
			t.Error("mapper Next() is not ok")
		}
		if err != nil {
			t.Fatal("mapper Next() error:", err)
		}

		if t1.Name != expectedMap[t1.Id] {
			t.Errorf("t1.Name is not %s: %s\n", expectedMap[t1.Id], t1.Name)
		}
	}

	if ok, _ := mapper.Next(new(Table1)); ok {
		t.Error("mapper Next() is ok")
	}
}
