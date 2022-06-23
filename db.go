package mini_kv_db

import (
	"io"
	"os"
	"sync"
)

const (
	SET uint16 = 0
	DEL uint16 = 1
)

type MiniKVDB struct {
	indexes map[string]int64 //内存中保存的文件真实的物理索引位置
	dbFile  *DbFile          //数据文件
	dirPath string           //数据目录
	mu      sync.RWMutex     //读写同步锁
}

//
// Open
// @Description: Open a MiniKVDB instance
// @param dirPath
// @return *MiniKVDB
// @return error
//
func Open(dirPath string) (*MiniKVDB, error) {
	_, err := os.Stat(dirPath)
	{
		if os.IsNotExist(err) {
			err := os.MkdirAll(dirPath, os.ModePerm)
			if err != nil {
				return nil, err
			}
		}
	}

	//加载数据文件

	dbFile, err := NewDbFile(dirPath)
	if err != nil {
		return nil, err
	}
	db := &MiniKVDB{
		dbFile:  dbFile,
		dirPath: dirPath,
		indexes: make(map[string]int64),
	}

	//加载索引
	db.loadIndexesFromFile()
	return db, err

}

//
// Merge
// @Description: 合并数据文件
// @receiver db
//
func (db *MiniKVDB) Merge() error {
	if db.dbFile.Offset == 0 {
		return nil
	}
	var (
		validEntries []*Entry
		offset       int64
	)
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		//?
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	if len(validEntries) <= 0 {
		return nil
	}
	//新建临时文件
	mergeDbFile, err := NewMergeDbFile(db.dirPath)
	if err != nil {
		return err
	}
	defer os.Remove(mergeDbFile.File.Name())

	//重新写入有效的entry
	for _, entry := range validEntries {
		writeOffset := mergeDbFile.Offset
		err := mergeDbFile.Write(entry)
		if err != nil {
			return err
		}
		//更新索引
		db.indexes[string(entry.Key)] = writeOffset
	}
	//获取文件名
	dbFileName := db.dbFile.File.Name()
	db.dbFile.File.Close()
	//删除旧的数据文件
	os.Remove(dbFileName)
	//合并好的文件名
	mergeDbFileName := mergeDbFile.File.Name()
	mergeDbFile.File.Close()
	os.Rename(mergeDbFileName, db.dirPath+string(os.PathSeparator)+FileName)
	db.dbFile = mergeDbFile
	return nil
}

func (db *MiniKVDB) Set(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	offset := db.dbFile.Offset
	//封装成entry
	entry := NewEntry(key, value, SET)

	err = db.dbFile.Write(entry)
	if err != nil {
		return err
	}

	db.indexes[string(key)] = offset
	return

}

func (db *MiniKVDB) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	//从内存中去除索引
	offset, ok := db.indexes[string(key)]
	if !ok {
		//key不存在
		return
	}
	entry, err := db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if entry != nil {
		val = entry.Value
	}

	return
}

//
// Del
// @Description: 删除数据
// @receiver db
// @param key
// @return error
//
func (db *MiniKVDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	//如果内存中已经没有,则直接返回
	_, ok := db.indexes[string(key)]
	if !ok {
		return
	}

	//Step1往文件写一个删除的entry
	entry := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(entry)
	if err != nil {
		return err
	}

	//Step2删除内存中的数据
	delete(db.indexes, string(key))

	return nil
}

func (db *MiniKVDB) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			//读取完毕
			if err == io.EOF {
				break
			}
			return
		}
		//设置索引状态
		db.indexes[string(e.Key)] = offset
		if e.Mark == DEL {
			delete(db.indexes, string(e.Key))
		}
		offset = offset + e.GetSize()
	}
}
