package mini_kv_db

import (
	"reflect"
	"testing"
)

func TestOpen(t *testing.T) {
	miniKVDB, err := Open("/tmp/minikvdb")
	if err != nil {
		t.Error(err)
	}
	t.Log(miniKVDB)
}

func TestMiniKVDB_Set(t *testing.T) {
	miniKVDB, err := Open("/tmp/minikvdb")
	if err != nil {
		t.Error(err)
	}

	err = miniKVDB.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Error(err)
	}
}

//
// TestMiniKVDB_Get
// @Description: 测试这个方法前要先测试set
// @param t
//
func TestMiniKVDB_Get(t *testing.T) {
	miniKVDB, err := Open("/tmp/minikvdb")
	if err != nil {
		t.Error(err)
	}
	bytes, err := miniKVDB.Get([]byte("key1"))
	if err != nil {
		t.Error(err)
	}

	equal := reflect.DeepEqual(bytes, []byte("value1"))
	if !equal {
		t.Error("not equal")
	}

}

func TestMiniKVDB_Del(t *testing.T) {
	miniKVDB, err := Open("/tmp/minikvdb")
	if err != nil {
		t.Error(err)
	}

	err = miniKVDB.Del([]byte("key1"))
	if err != nil {
		t.Error(err)
	}
}

func TestMiniKVDB_Merge(t *testing.T) {
	miniKVDB, err := Open("/tmp/minikvdb")
	if err != nil {
		t.Error(err)
	}
	err = miniKVDB.Merge()
	if err != nil {
		t.Error("merge err", err)
	}
}
