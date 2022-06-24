package mini_kv_db

import "os"

const FileName = "minikvdb.data"
const MergeFileName = "minikvdb.data.merge"

type DbFile struct {
	File   *os.File
	Offset int64
}

//
// newInternal
// @Description: 新建一个文件
// @param fileName
// @return *DbFile
// @return error
//
func newInternal(fileName string) (*DbFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	return &DbFile{File: file, Offset: stat.Size()}, nil
}

//
// NewDbFile
// @Description: 新建一个数据文件
// @param path
// @return *DbFile
// @return error
//
func NewDbFile(path string) (*DbFile, error) {
	fileName := path + string(os.PathSeparator) + FileName
	return newInternal(fileName)
}

//
// NewMergeDbFile
// @Description: 新建一个合并数据文件
// @param path
// @return *DbFile
// @return error
//
func NewMergeDbFile(path string) (*DbFile, error) {
	fileName := path + string(os.PathSeparator) + MergeFileName
	return newInternal(fileName)
}

//
// Read
// @Description: 读指定偏移位置的Entry
// @receiver df
// @param offset 偏移量
// @return entry
// @return err
//
func (df *DbFile) Read(offset int64) (entry *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}

	if entry, err = Decode(buf); err != nil {
		return
	}

	offset = offset + entryHeaderSize
	if entry.KeySize > 0 {
		key := make([]byte, entry.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		entry.Key = key
	}

	offset = offset + int64(entry.KeySize)

	if entry.ValueSize > 0 {
		value := make([]byte, entry.ValueSize)
		_, err := df.File.ReadAt(value, offset)
		if err != nil {
			return nil, err
		}
		entry.Value = value
	}
	return entry, nil
}

//
// Write
// @Description: 把一个Entry写入数据文件
// @receiver df
// @param e
// @return err
//
func (df *DbFile) Write(e *Entry) (err error) {
	encodeOne, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(encodeOne, df.Offset)
	if err != nil {
		return
	}
	df.Offset = df.Offset + e.GetSize()
	return

}
