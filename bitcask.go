package bitcask

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/abcum/lcp"
	"github.com/gofrs/flock"
	art "github.com/plar/go-adaptive-radix-tree"
	log "github.com/sirupsen/logrus"

	"git.mills.io/prologic/bitcask/internal"
	"git.mills.io/prologic/bitcask/internal/config"
	"git.mills.io/prologic/bitcask/internal/data"
	"git.mills.io/prologic/bitcask/internal/data/codec"
	"git.mills.io/prologic/bitcask/internal/index"
	"git.mills.io/prologic/bitcask/internal/metadata"
	"git.mills.io/prologic/bitcask/scripts/migrations"
)

const (
	lockfile     = "lock"
	ttlIndexFile = "ttl_index"
)

// Bitcask is a struct that represents a on-disk LSM and WAL data structure
// and in-memory hash of key/value pairs as per the Bitcask paper and seen
// in the Riak database.
type Bitcask struct {
	mu         sync.RWMutex
	flock      *flock.Flock
	config     *config.Config
	options    []Option
	path       string
	curr       data.Datafile
	datafiles  map[int]data.Datafile
	trie       art.Tree
	indexer    index.Indexer
	ttlIndexer index.Indexer
	ttlIndex   art.Tree
	metadata   *metadata.MetaData
	isMerging  bool
}

// Stats is a struct returned by Stats() on an open Bitcask instance
type Stats struct {
	Datafiles int
	Keys      int
	Size      int64
}

// Stats returns statistics about the database including the number of
// data files, keys and overall size on disk of the data
func (b *Bitcask) Stats() (stats Stats, err error) {
	if stats.Size, err = internal.DirSize(b.path); err != nil {
		return
	}

	b.mu.RLock()
	stats.Datafiles = len(b.datafiles)
	stats.Keys = b.trie.Size()
	b.mu.RUnlock()

	return
}

// Close closes the database and removes the lock. It is important to call
// Close() as this is the only way to cleanup the lock held by the open
// database.
func (b *Bitcask) Close() error {
	b.mu.RLock()
	defer func() {
		b.mu.RUnlock()
		b.flock.Unlock()
	}()

	return b.close()
}

func (b *Bitcask) close() error {
	// 关闭db时：保存内存索引
	if err := b.saveIndexes(); err != nil {
		return err
	}

	b.metadata.IndexUpToDate = true
	// 保存db元数据信息
	if err := b.saveMetadata(); err != nil {
		return err
	}
	// 将所有的old文件关闭
	for _, df := range b.datafiles {
		if err := df.Close(); err != nil {
			return err
		}
	}

	return b.curr.Close()
}

// Sync flushes all buffers to disk ensuring all data is written
func (b *Bitcask) Sync() error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.saveMetadata(); err != nil {
		return err
	}

	return b.curr.Sync()
}

// Get fetches value for a key
func (b *Bitcask) Get(key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	e, err := b.get(key)
	if err != nil {
		return nil, err
	}
	return e.Value, nil
}

// Has returns true if the key exists in the database, false otherwise.
func (b *Bitcask) Has(key []byte) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// 通过在内存索引中查找即可判断是否有此key
	_, found := b.trie.Search(key)
	if found {
		return !b.isExpired(key)
	}
	return found
}

// Put stores the key and value in the database.
func (b *Bitcask) Put(key, value []byte) error {
	// 对key的校验，非空
	if len(key) == 0 {
		return ErrEmptyKey
	}
	// 对key，value的大小的限制
	if b.config.MaxKeySize > 0 && uint32(len(key)) > b.config.MaxKeySize {
		return ErrKeyTooLarge
	}
	if b.config.MaxValueSize > 0 && uint64(len(value)) > b.config.MaxValueSize {
		return ErrValueTooLarge
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// 写入活跃文件后的offset与size
	offset, n, err := b.put(key, value)
	if err != nil {
		return err
	}
	// 如果强制落盘（每次put直接写磁盘）
	if b.config.Sync {
		// 将当前活跃文件内的缓冲数据全部刷到磁盘
		if err := b.curr.Sync(); err != nil {
			return err
		}
	}

	// in case of successful `put`, IndexUpToDate will be always be false
	b.metadata.IndexUpToDate = false
	// 需要更新内存索引
	if oldItem, found := b.trie.Search(key); found {
		// TODO ReclaimableSpace 增加item的大小，这个参数什么用
		// 用来评估 是否需要merge
		b.metadata.ReclaimableSpace += oldItem.(internal.Item).Size
	}
	// 建立内存索引
	item := internal.Item{FileID: b.curr.FileID(), Offset: offset, Size: n}
	b.trie.Insert(key, item)

	return nil
}

// PutWithTTL stores the key and value in the database with the given TTL
func (b *Bitcask) PutWithTTL(key, value []byte, ttl time.Duration) error {
	// 与常规put一样，进行kv的检验
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if b.config.MaxKeySize > 0 && uint32(len(key)) > b.config.MaxKeySize {
		return ErrKeyTooLarge
	}
	if b.config.MaxValueSize > 0 && uint64(len(value)) > b.config.MaxValueSize {
		return ErrValueTooLarge
	}
	// 增加一个过期时间戳
	expiry := time.Now().Add(ttl)

	b.mu.Lock()
	defer b.mu.Unlock()
	// 先写入文件
	offset, n, err := b.putWithExpiry(key, value, expiry)
	if err != nil {
		return err
	}

	if b.config.Sync {
		if err := b.curr.Sync(); err != nil {
			return err
		}
	}

	// in case of successful `put`, IndexUpToDate will be always be false
	b.metadata.IndexUpToDate = false

	if oldItem, found := b.trie.Search(key); found {
		b.metadata.ReclaimableSpace += oldItem.(internal.Item).Size
	}
	// 加入内存索引art树
	item := internal.Item{FileID: b.curr.FileID(), Offset: offset, Size: n}
	b.trie.Insert(key, item)
	// 在ttlIndex索引树中也添加一条记录
	b.ttlIndex.Insert(key, expiry)

	return nil
}

// Delete deletes the named key.
func (b *Bitcask) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.delete(key)
}

// delete deletes the named key. If the key doesn't exist or an I/O error
// occurs the error is returned.
func (b *Bitcask) delete(key []byte) error {
	// delete依赖put方法，通过将value设置为空，标记此记录为删除操作
	_, _, err := b.put(key, []byte{})
	if err != nil {
		return err
	}
	if item, found := b.trie.Search(key); found {
		b.metadata.ReclaimableSpace += item.(internal.Item).Size + codec.MetaInfoSize + int64(len(key))
	}
	// 内存索引中删除key
	b.trie.Delete(key)
	b.ttlIndex.Delete(key)

	return nil
}

// Sift iterates over all keys in the database calling the function `f` for
// each key. If the KV pair is expired or the function returns true, that key is
// deleted from the database.
// If the function returns an error on any key, no further keys are processed, no
// keys are deleted, and the first error is returned.
func (b *Bitcask) Sift(f func(key []byte) (bool, error)) (err error) {
	keysToDelete := art.New()

	b.mu.RLock()
	b.trie.ForEach(func(node art.Node) bool {
		if b.isExpired(node.Key()) {
			keysToDelete.Insert(node.Key(), true)
			return true
		}
		var shouldDelete bool
		if shouldDelete, err = f(node.Key()); err != nil {
			return false
		} else if shouldDelete {
			keysToDelete.Insert(node.Key(), true)
		}
		return true
	})
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	keysToDelete.ForEach(func(node art.Node) (cont bool) {
		b.delete(node.Key())
		return true
	})
	return
}

// DeleteAll deletes all the keys. If an I/O error occurs the error is returned.
func (b *Bitcask) DeleteAll() (err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEach(func(node art.Node) bool {
		_, _, err = b.put(node.Key(), []byte{})
		if err != nil {
			return false
		}
		item, _ := b.trie.Search(node.Key())
		b.metadata.ReclaimableSpace += item.(internal.Item).Size + codec.MetaInfoSize + int64(len(node.Key()))
		return true
	})
	b.trie = art.New()
	b.ttlIndex = art.New()

	return
}

// Scan performs a prefix scan of keys matching the given prefix and calling
// the function `f` with the keys found. If the function returns an error
// no further keys are processed and the first error is returned.
func (b *Bitcask) Scan(prefix []byte, f func(key []byte) error) (err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEachPrefix(prefix, func(node art.Node) bool {
		// Skip the root node
		if len(node.Key()) == 0 {
			return true
		}

		if err = f(node.Key()); err != nil {
			return false
		}
		return true
	})
	return
}

// SiftScan iterates over all keys in the database beginning with the given
// prefix, calling the function `f` for each key. If the KV pair is expired or
// the function returns true, that key is deleted from the database.
//  If the function returns an error on any key, no further keys are processed,
// no keys are deleted, and the first error is returned.
func (b *Bitcask) SiftScan(prefix []byte, f func(key []byte) (bool, error)) (err error) {
	keysToDelete := art.New()

	b.mu.RLock()
	b.trie.ForEachPrefix(prefix, func(node art.Node) bool {
		// Skip the root node
		if len(node.Key()) == 0 {
			return true
		}
		if b.isExpired(node.Key()) {
			keysToDelete.Insert(node.Key(), true)
			return true
		}
		var shouldDelete bool
		if shouldDelete, err = f(node.Key()); err != nil {
			return false
		} else if shouldDelete {
			keysToDelete.Insert(node.Key(), true)
		}
		return true
	})
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	keysToDelete.ForEach(func(node art.Node) (cont bool) {
		b.delete(node.Key())
		return true
	})
	return
}

// Range performs a range scan of keys matching a range of keys between the
// start key and end key and calling the function `f` with the keys found.
// If the function returns an error no further keys are processed and the
// first error returned.
func (b *Bitcask) Range(start, end []byte, f func(key []byte) error) (err error) {
	if bytes.Compare(start, end) == 1 {
		return ErrInvalidRange
	}

	commonPrefix := lcp.LCP(start, end)
	if commonPrefix == nil {
		return ErrInvalidRange
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEachPrefix(commonPrefix, func(node art.Node) bool {
		if bytes.Compare(node.Key(), start) >= 0 && bytes.Compare(node.Key(), end) <= 0 {
			if err = f(node.Key()); err != nil {
				return false
			}
			return true
		} else if bytes.Compare(node.Key(), start) >= 0 && bytes.Compare(node.Key(), end) > 0 {
			return false
		}
		return true
	})
	return
}

// SiftRange performs a range scan of keys matching a range of keys between the
// start key and end key and calling the function `f` with the keys found.
// If the KV pair is expired or the function returns true, that key is deleted
// from the database.
// If the function returns an error on any key, no further keys are processed, no
// keys are deleted, and the first error is returned.
func (b *Bitcask) SiftRange(start, end []byte, f func(key []byte) (bool, error)) (err error) {
	if bytes.Compare(start, end) == 1 {
		return ErrInvalidRange
	}

	commonPrefix := lcp.LCP(start, end)
	if commonPrefix == nil {
		return ErrInvalidRange
	}

	keysToDelete := art.New()

	b.mu.RLock()
	b.trie.ForEachPrefix(commonPrefix, func(node art.Node) bool {
		if bytes.Compare(node.Key(), start) >= 0 && bytes.Compare(node.Key(), end) <= 0 {
			if b.isExpired(node.Key()) {
				keysToDelete.Insert(node.Key(), true)
				return true
			}
			var shouldDelete bool
			if shouldDelete, err = f(node.Key()); err != nil {
				return false
			} else if shouldDelete {
				keysToDelete.Insert(node.Key(), true)
			}
			return true
		} else if bytes.Compare(node.Key(), start) >= 0 && bytes.Compare(node.Key(), end) > 0 {
			return false
		}
		return true
	})
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	keysToDelete.ForEach(func(node art.Node) (cont bool) {
		b.delete(node.Key())
		return true
	})

	return
}

// Len returns the total number of keys in the database
func (b *Bitcask) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// 获得keys的数目，直接调用tire的size方法
	return b.trie.Size()
}

// Keys returns all keys in the database as a channel of keys
func (b *Bitcask) Keys() chan []byte {
	ch := make(chan []byte)
	go func() {
		b.mu.RLock()
		defer b.mu.RUnlock()

		for it := b.trie.Iterator(); it.HasNext(); {
			node, _ := it.Next()
			if b.isExpired(node.Key()) {
				continue
			}
			ch <- node.Key()
		}
		close(ch)
	}()

	return ch
}

// RunGC deletes all expired keys
func (b *Bitcask) RunGC() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.runGC()
}

// runGC deletes all keys that are expired
// caller function should take care of the locking when calling this method
func (b *Bitcask) runGC() (err error) {
	keysToDelete := art.New()

	b.ttlIndex.ForEach(func(node art.Node) (cont bool) {
		if !b.isExpired(node.Key()) {
			// later, return false here when the ttlIndex is sorted
			return true
		}
		keysToDelete.Insert(node.Key(), true)
		//keysToDelete = append(keysToDelete, node.Key())
		return true
	})
	// 删除过期key
	keysToDelete.ForEach(func(node art.Node) (cont bool) {
		b.delete(node.Key())
		return true
	})

	return nil
}

// Fold iterates over all keys in the database calling the function `f` for
// each key. If the function returns an error, no further keys are processed
// and the error is returned.
func (b *Bitcask) Fold(f func(key []byte) error) (err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEach(func(node art.Node) bool {
		if err = f(node.Key()); err != nil {
			return false
		}
		return true
	})

	return
}

// get retrieves the value of the given key
func (b *Bitcask) get(key []byte) (internal.Entry, error) {
	var df data.Datafile
	// 先从内存索引art树中找item
	value, found := b.trie.Search(key)
	if !found {
		return internal.Entry{}, ErrKeyNotFound
	}
	// 判断是否已过期
	if b.isExpired(key) {
		return internal.Entry{}, ErrKeyExpired
	}

	item := value.(internal.Item)
	// 判断此item所在的文件是否是当前活跃文件
	if item.FileID == b.curr.FileID() {
		df = b.curr
	} else {
		df = b.datafiles[item.FileID]
	}
	// 找到对应的文件，读取entry
	e, err := df.ReadAt(item.Offset, item.Size)
	if err != nil {
		return internal.Entry{}, err
	}
	// 进行校验
	checksum := crc32.ChecksumIEEE(e.Value)
	if checksum != e.Checksum {
		return internal.Entry{}, ErrChecksumFailed
	}

	return e, nil
}

func (b *Bitcask) maybeRotate() error {
	// 判断文件大小是否已达上限，需要关闭
	size := b.curr.Size()
	if size < int64(b.config.MaxDatafileSize) {
		return nil
	}
	// 关闭旧文件
	err := b.curr.Close()
	if err != nil {
		return err
	}
	// 获取关闭的文件id
	id := b.curr.FileID()
	// 创建一个readonly文件，已关闭的只读文件
	df, err := data.NewDatafile(
		b.path, id, true,
		b.config.MaxKeySize,
		b.config.MaxValueSize,
		b.config.FileFileModeBeforeUmask,
	)
	if err != nil {
		return err
	}
	// 加入dataFiles列表
	b.datafiles[id] = df

	id = b.curr.FileID() + 1
	// 创建一个新的活跃文件
	curr, err := data.NewDatafile(
		b.path, id, false,
		b.config.MaxKeySize,
		b.config.MaxValueSize,
		b.config.FileFileModeBeforeUmask,
	)
	if err != nil {
		return err
	}
	// 赋值为当前活跃文件
	b.curr = curr
	// TODO 写内存索引，需要详细看
	err = b.saveIndexes()
	if err != nil {
		return err
	}

	return nil
}

// put inserts a new (key, value). Both key and value are valid inputs.
func (b *Bitcask) put(key, value []byte) (int64, int64, error) {
	// 校验：是否需要关闭旧文件，创建新的datafile
	if err := b.maybeRotate(); err != nil {
		return -1, 0, fmt.Errorf("error rotating active datafile: %w", err)
	}
	// 写入当前的活跃文件，返回offset，size
	return b.curr.Write(internal.NewEntry(key, value, nil))
}

// putWithExpiry inserts a new (key, value, expiry).
// Both key and value are valid inputs.
func (b *Bitcask) putWithExpiry(key, value []byte, expiry time.Time) (int64, int64, error) {
	if err := b.maybeRotate(); err != nil {
		return -1, 0, fmt.Errorf("error rotating active datafile: %w", err)
	}
	// 注意加入expiry字段
	return b.curr.Write(internal.NewEntry(key, value, &expiry))
}

// closeCurrentFile closes current datafile and makes it read only.
func (b *Bitcask) closeCurrentFile() error {
	if err := b.curr.Close(); err != nil {
		return err
	}
	// 获取当前活跃文件的id
	id := b.curr.FileID()
	// 使其变为只读文件
	df, err := data.NewDatafile(
		b.path, id, true,
		b.config.MaxKeySize,
		b.config.MaxValueSize,
		b.config.FileFileModeBeforeUmask,
	)
	if err != nil {
		return err
	}
	// 加入关闭文件列表
	b.datafiles[id] = df
	return nil
}

// openNewWritableFile opens new datafile for writing data
func (b *Bitcask) openNewWritableFile() error {
	id := b.curr.FileID() + 1
	curr, err := data.NewDatafile(
		b.path, id, false,
		b.config.MaxKeySize,
		b.config.MaxValueSize,
		b.config.FileFileModeBeforeUmask,
	)
	if err != nil {
		return err
	}
	b.curr = curr
	return nil
}

// Reopen closes and reopsns the database
func (b *Bitcask) Reopen() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.reopen()
}

// reopen reloads a bitcask object with index and datafiles
// caller of this method should take care of locking
func (b *Bitcask) reopen() error {
	datafiles, lastID, err := loadDatafiles(
		b.path,
		b.config.MaxKeySize,
		b.config.MaxValueSize,
		b.config.FileFileModeBeforeUmask,
	)
	if err != nil {
		return err
	}
	t, ttlIndex, err := loadIndexes(b, datafiles, lastID)
	if err != nil {
		return err
	}

	curr, err := data.NewDatafile(
		b.path, lastID, false,
		b.config.MaxKeySize,
		b.config.MaxValueSize,
		b.config.FileFileModeBeforeUmask,
	)
	if err != nil {
		return err
	}

	b.trie = t
	b.curr = curr
	b.ttlIndex = ttlIndex
	b.datafiles = datafiles

	return nil
}

// Merge merges all datafiles in the database. Old keys are squashed
// and deleted keys removes. Duplicate key/value pairs are also removed.
// Call this function periodically to reclaim disk space.
func (b *Bitcask) Merge() error {
	//TODO 重点关注merge过程
	b.mu.Lock()
	if b.isMerging {
		b.mu.Unlock()
		return ErrMergeInProgress
	}
	b.isMerging = true
	b.mu.Unlock()
	defer func() {
		b.isMerging = false
	}()
	b.mu.RLock()
	// 将当前活跃文件设置为只读
	err := b.closeCurrentFile()
	if err != nil {
		b.mu.RUnlock()
		return err
	}
	// 当前db索引--》对应当前数据库内的数据文件
	// （需要为当前db创建一个新的可写文件，不妨碍写入）
	// 创建一个新的临时db--》会创建一个active文件，用于写数据
	// 遍历当前的db索引，访问当前db内的数据文件中，将kv写入临时db中
	// 删除之前的数据文件，将临时db的active文件移动到当前db的数据文件目录中，重新打开，完成合并
	filesToMerge := make([]int, 0, len(b.datafiles))
	for k := range b.datafiles {
		filesToMerge = append(filesToMerge, k)
	}
	// 创建一个新的可写文件，作为active文件
	err = b.openNewWritableFile()
	if err != nil {
		b.mu.RUnlock()
		return err
	}
	b.mu.RUnlock()
	sort.Ints(filesToMerge)

	// Temporary merged database path
	temp, err := ioutil.TempDir(b.path, "merge")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)

	// Create a merged database
	mdb, err := Open(temp, withConfig(b.config))
	if err != nil {
		return err
	}

	// Rewrite all key/value pairs into merged database
	// Doing this automatically strips deleted keys and
	// old key/value pairs
	err = b.Fold(func(key []byte) error {
		item, _ := b.trie.Search(key)
		// if key was updated after start of merge operation, nothing to do
		if item.(internal.Item).FileID > filesToMerge[len(filesToMerge)-1] {
			return nil
		}
		e, err := b.get(key)
		if err != nil {
			return err
		}

		if e.Expiry != nil {
			if err := mdb.PutWithTTL(key, e.Value, time.Until(*e.Expiry)); err != nil {
				return err
			}
		} else {
			if err := mdb.Put(key, e.Value); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}
	if err = mdb.Close(); err != nil {
		return err
	}
	// no reads and writes till we reopen
	b.mu.Lock()
	defer b.mu.Unlock()
	if err = b.close(); err != nil {
		return err
	}

	// Remove data files
	files, err := ioutil.ReadDir(b.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() || file.Name() == lockfile {
			continue
		}
		ids, err := internal.ParseIds([]string{file.Name()})
		if err != nil {
			return err
		}
		// if datafile was created after start of merge, skip
		if len(ids) > 0 && ids[0] > filesToMerge[len(filesToMerge)-1] {
			continue
		}
		err = os.RemoveAll(path.Join(b.path, file.Name()))
		if err != nil {
			return err
		}
	}

	// Rename all merged data files
	files, err = ioutil.ReadDir(mdb.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		// see #225
		if file.Name() == lockfile {
			continue
		}
		err := os.Rename(
			path.Join([]string{mdb.path, file.Name()}...),
			path.Join([]string{b.path, file.Name()}...),
		)
		if err != nil {
			return err
		}
	}
	b.metadata.ReclaimableSpace = 0

	// And finally reopen the database
	return b.reopen()
}

// Open opens the database at the given path with optional options.
// Options can be provided with the `WithXXX` functions that provide
// configuration options as functions.
// 打开数据库，使用options模式进行配置记载
func Open(path string, options ...Option) (*Bitcask, error) {
	var (
		cfg  *config.Config
		err  error
		meta *metadata.MetaData
	)

	configPath := filepath.Join(path, "config.json")
	// 先尝试读取默认的配置文件
	if internal.Exists(configPath) {
		cfg, err = config.Load(configPath)
		if err != nil {
			return nil, &ErrBadConfig{err}
		}
	} else {
		// 若不存在配置文件，则使用默认配置
		cfg = newDefaultConfig()
	}
	// 判断是否需要版本升级
	if err := checkAndUpgrade(cfg, configPath); err != nil {
		return nil, err
	}
	// 使用option模式进行初始化
	for _, opt := range options {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	// 创建文件夹，用于存放数据库的文件
	if err := os.MkdirAll(path, cfg.DirFileModeBeforeUmask); err != nil {
		return nil, err
	}
	// TODO 加载数据库元数据？与配置信息有什么不同？
	meta, err = loadMetadata(path)
	if err != nil {
		return nil, &ErrBadMetadata{err}
	}

	bitcask := &Bitcask{
		// TODO 文件锁，干什么用的
		flock:      flock.New(filepath.Join(path, lockfile)),
		config:     cfg,
		options:    options,
		path:       path,
		// 内存索引 indexer
		indexer:    index.NewIndexer(),
		// ttlIndexer 用来存有过期时间的索引
		ttlIndexer: index.NewTTLIndexer(),
		metadata:   meta,
	}

	ok, err := bitcask.flock.TryLock()
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, ErrDatabaseLocked
	}
	// 将配置信息写入配置文件
	if err := cfg.Save(configPath); err != nil {
		return nil, err
	}

	if cfg.AutoRecovery {
		if err := data.CheckAndRecover(path, cfg); err != nil {
			return nil, fmt.Errorf("recovering database: %s", err)
		}
	}
	if err := bitcask.Reopen(); err != nil {
		return nil, err
	}

	return bitcask, nil
}

// checkAndUpgrade checks if DB upgrade is required
// if yes, then applies version upgrade and saves updated config
func checkAndUpgrade(cfg *config.Config, configPath string) error {
	if cfg.DBVersion == CurrentDBVersion {
		return nil
	}
	if cfg.DBVersion > CurrentDBVersion {
		return ErrInvalidVersion
	}
	// for v0 to v1 upgrade, we need to append 8 null bytes after each encoded entry in datafiles
	if cfg.DBVersion == uint32(0) && CurrentDBVersion == uint32(1) {
		log.Warn("upgrading db version, might take some time....")
		cfg.DBVersion = CurrentDBVersion
		return migrations.ApplyV0ToV1(filepath.Dir(configPath), cfg.MaxDatafileSize)
	}
	return nil
}

// Backup copies db directory to given path
// it creates path if it does not exist
func (b *Bitcask) Backup(path string) error {
	if !internal.Exists(path) {
		if err := os.MkdirAll(path, b.config.DirFileModeBeforeUmask); err != nil {
			return err
		}
	}
	return internal.Copy(b.path, path, []string{lockfile})
}

// saveIndex saves index and ttl_index currently in RAM to disk
// 在每次put操作都会调用，在close db之前也会调用
func (b *Bitcask) saveIndexes() error {
	tempIdx := "temp_index"
	// 将内存索引写入临时文件
	if err := b.indexer.Save(b.trie, filepath.Join(b.path, tempIdx)); err != nil {
		return err
	}
	// todo 为什么要先写临时文件，再rename？参考redis的数据持久化方案
	if err := os.Rename(filepath.Join(b.path, tempIdx), filepath.Join(b.path, "index")); err != nil {
		return err
	}
	if err := b.ttlIndexer.Save(b.ttlIndex, filepath.Join(b.path, tempIdx)); err != nil {
		return err
	}
	return os.Rename(filepath.Join(b.path, tempIdx), filepath.Join(b.path, ttlIndexFile))
}

// saveMetadata saves metadata into disk
func (b *Bitcask) saveMetadata() error {
	return b.metadata.Save(filepath.Join(b.path, "meta.json"), b.config.DirFileModeBeforeUmask)
}

// Reclaimable returns space that can be reclaimed
func (b *Bitcask) Reclaimable() int64 {
	return b.metadata.ReclaimableSpace
}

// isExpired returns true if a key has expired
// it returns false if key does not exist in ttl index
func (b *Bitcask) isExpired(key []byte) bool {
	expiry, found := b.ttlIndex.Search(key)
	if !found {
		return false
	}
	return expiry.(time.Time).Before(time.Now().UTC())
}

func loadDatafiles(path string, maxKeySize uint32, maxValueSize uint64, fileModeBeforeUmask os.FileMode) (datafiles map[int]data.Datafile, lastID int, err error) {
	fns, err := internal.GetDatafiles(path)
	if err != nil {
		return nil, 0, err
	}

	ids, err := internal.ParseIds(fns)
	if err != nil {
		return nil, 0, err
	}
	// 所有old文件的文件id列表
	datafiles = make(map[int]data.Datafile, len(ids))
	for _, id := range ids {
		datafiles[id], err = data.NewDatafile(
			path, id, true,
			maxKeySize,
			maxValueSize,
			fileModeBeforeUmask,
		)
		if err != nil {
			return
		}

	}
	if len(ids) > 0 {
		lastID = ids[len(ids)-1]
	}
	return
}

func getSortedDatafiles(datafiles map[int]data.Datafile) []data.Datafile {
	out := make([]data.Datafile, len(datafiles))
	idx := 0
	for _, df := range datafiles {
		out[idx] = df
		idx++
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FileID() < out[j].FileID()
	})
	// 将所有old文件根据id进行排序
	return out
}

// loadIndexes loads index from disk to memory. If index is not available or partially available (last bitcask process crashed)
// then it iterates over last datafile and construct index
// we construct ttl_index here also along with normal index
func loadIndexes(b *Bitcask, datafiles map[int]data.Datafile, lastID int) (art.Tree, art.Tree, error) {
	t, found, err := b.indexer.Load(filepath.Join(b.path, "index"), b.config.MaxKeySize)
	if err != nil {
		return nil, nil, err
	}
	ttlIndex, _, err := b.ttlIndexer.Load(filepath.Join(b.path, ttlIndexFile), b.config.MaxKeySize)
	if err != nil {
		return nil, nil, err
	}
	if found && b.metadata.IndexUpToDate {
		return t, ttlIndex, nil
	}
	if found {
		// lastID对应的就是活跃文件，需要将活跃文件的数据也建立索引
		if err := loadIndexFromDatafile(t, ttlIndex, datafiles[lastID]); err != nil {
			return nil, ttlIndex, err
		}
		return t, ttlIndex, nil
	}
	// 如果加载到内存的索引不是最新的，上次关闭前没有完全保存？那么需要重读datafiles，重建索引
	sortedDatafiles := getSortedDatafiles(datafiles)
	for _, df := range sortedDatafiles {
		if err := loadIndexFromDatafile(t, ttlIndex, df); err != nil {
			return nil, ttlIndex, err
		}
	}
	return t, ttlIndex, nil
}

func loadIndexFromDatafile(t art.Tree, ttlIndex art.Tree, df data.Datafile) error {
	var offset int64
	for {
		e, n, err := df.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// Tombstone value  (deleted key)
		if len(e.Value) == 0 {
			t.Delete(e.Key)
			offset += n
			continue
		}
		item := internal.Item{FileID: df.FileID(), Offset: offset, Size: n}
		t.Insert(e.Key, item)
		if e.Expiry != nil {
			ttlIndex.Insert(e.Key, *e.Expiry)
		}
		offset += n
	}
	return nil
}

func loadMetadata(path string) (*metadata.MetaData, error) {
	if !internal.Exists(filepath.Join(path, "meta.json")) {
		meta := new(metadata.MetaData)
		return meta, nil
	}
	return metadata.Load(filepath.Join(path, "meta.json"))
}
