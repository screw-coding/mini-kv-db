package mini_kv_db

import "encoding/binary"

// 入口的头大小 10个字节,4字节存key大小,4字节存value大小,2字节存mark标记,刚好10字节
const entryHeaderSize = 10

//
// Entry
// @Description: 每个操作的日志结构
// @param key 键
//
type Entry struct {
	//uint32:32bit=4byte
	KeySize   uint32
	ValueSize uint32
	//uint16:16bit=2byte
	Mark  uint16
	Key   []byte
	Value []byte
}

//
// NewEntry
// @Description: 获取一个新的入口
// @param key
// @param value
// @param mark
// @return *Entry
//
func NewEntry(key, value []byte, mark uint16) *Entry {
	return &Entry{
		Key:       key,
		KeySize:   uint32(int32(len(key))),
		Value:     value,
		ValueSize: uint32(int32(len(value))),
		Mark:      mark,
	}
}

//
// GetSize
// @Description: 获取一个entry实际需要的磁盘空间字节数
// @receiver e
// @return int64
//
func (e *Entry) GetSize() int64 {
	return int64(entryHeaderSize + e.KeySize + e.ValueSize)
}

//
// Encode
// @Description: 将Entry序列化为字节集
// @receiver e
// @return []byte
// @return error
//
func (e *Entry) Encode() ([]byte, error) {
	// 申请一块内存
	buf := make([]byte, e.GetSize())
	//第一位到第四位放key的大小
	binary.BigEndian.PutUint32(buf[0:4], e.KeySize)
	//第5位到第八位放value的大小
	binary.BigEndian.PutUint32(buf[4:8], e.ValueSize)
	//第9位第十位放mark标记的值
	binary.BigEndian.PutUint16(buf[8:10], e.Mark)
	copy(buf[entryHeaderSize:entryHeaderSize+e.KeySize], e.Key)
	copy(buf[entryHeaderSize+e.KeySize:], e.Value)
	return buf, nil
}

//
// Decode
// @Description: 将字节数据解析为Entry
// @param buf
// @return *Entry
// @return error
//
func Decode(buf []byte) (*Entry, error) {
	keySize := binary.BigEndian.Uint32(buf[0:4])
	valueSize := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])
	return &Entry{
		KeySize:   keySize,
		ValueSize: valueSize,
		Mark:      mark,
	}, nil
}
