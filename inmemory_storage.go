/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package inmemorystorage

import (
	"go.arpabet.com/storage"
	"io"
	"os"
	"github.com/patrickmn/go-cache"
	"strings"
	"time"
)

type inmemoryStorage struct {
	name      string
	cache     *cache.Cache
}

func NewDefault(name string) storage.ManagedStorage {
	return New(name)
}

func New(name string, options ...Option) storage.ManagedStorage {
	cache := OpenDatabase(options...)
	return &inmemoryStorage {name: name, cache: cache}
}

func FromCache(name string, c *cache.Cache) storage.ManagedStorage {
	return &inmemoryStorage {name: name, cache: c}
}

func (t* inmemoryStorage) BeanName() string {
	return t.name
}

func (t* inmemoryStorage) Destroy() error {
	return nil
}

func (t* inmemoryStorage) Get() *storage.GetOperation {
	return &storage.GetOperation{Storage: t}
}

func (t* inmemoryStorage) Set() *storage.SetOperation {
	return &storage.SetOperation{Storage: t}
}

func (t* inmemoryStorage) CompareAndSet() *storage.CompareAndSetOperation {
	return &storage.CompareAndSetOperation{Storage: t}
}

func (t *inmemoryStorage) Increment() *storage.IncrementOperation {
	return &storage.IncrementOperation{Storage: t, Initial: 0, Delta: 1}
}

func (t* inmemoryStorage) Remove() *storage.RemoveOperation {
	return &storage.RemoveOperation{Storage: t}
}

func (t* inmemoryStorage) Enumerate() *storage.EnumerateOperation {
	return &storage.EnumerateOperation{Storage: t}
}

func (t* inmemoryStorage) GetRaw(key []byte, ttlPtr *int, versionPtr *int64, required bool) ([]byte, error) {
	return t.getImpl(key, required)
}

func (t* inmemoryStorage) SetRaw(key, value []byte, ttlSeconds int) error {

	ttl := cache.NoExpiration
	if ttlSeconds > 0 {
		ttl = time.Second * time.Duration(ttlSeconds)
	}

	t.cache.Set(string(key), value, ttl)
	return nil
}

func (t *inmemoryStorage) DoInTransaction(key []byte, cb func(entry *storage.RawEntry) bool) error {

	rawEntry := &storage.RawEntry {
		Key: key,
		Ttl: storage.NoTTL,
		Version: 0,
	}

	if obj, ok := t.cache.Get(string(key)); ok && obj != nil {
		if b, ok := obj.([]byte); ok {
			rawEntry.Value = b
		}
	}

	if !cb(rawEntry) {
		return ErrCanceled
	}

	ttl := cache.NoExpiration
	if rawEntry.Ttl > 0 {
		ttl = time.Second * time.Duration(rawEntry.Ttl)
	}

	t.cache.Set(string(key), rawEntry.Value, ttl)
	return nil
}

func (t* inmemoryStorage) CompareAndSetRaw(key, value []byte, ttlSeconds int, version int64) (bool, error) {
	return true, t.SetRaw(key, value, ttlSeconds)
}

func (t* inmemoryStorage) RemoveRaw(key []byte) error {
	t.cache.Delete(string(key))
	return nil
}

func (t* inmemoryStorage) getImpl(key []byte, required bool) ([]byte, error) {

	var val []byte
	if obj, ok := t.cache.Get(string(key)); ok && obj != nil {
		if b, ok := obj.([]byte); ok {
			val = b
		}
	}

	if val == nil && required {
		return nil, os.ErrNotExist
	}

	return val, nil
}

func (t* inmemoryStorage) EnumerateRaw(prefix, seek []byte, batchSize int, onlyKeys bool, cb func(entry *storage.RawEntry) bool) error {

	prefixStr := string(prefix)
	seekStr := string(seek)

	for key, item := range t.cache.Items() {

		if val, ok := item.Object.([]byte); ok && strings.HasPrefix(key, prefixStr) && key >= seekStr {
			re := storage.RawEntry{
				Key:     []byte(key),
				Ttl:     int(item.Expiration),
				Version: item.Expiration,
			}
			if !onlyKeys {
				re.Value = val
			}
			if !cb(&re) {
				break
			}
		}

	}

	return nil
}

func (t* inmemoryStorage) Compact(discardRatio float64) error {
	t.cache.DeleteExpired()
	return nil
}

func (t* inmemoryStorage) Backup(w io.Writer, since uint64) (uint64, error) {
	return 0, t.cache.Save(w)
}

func (t* inmemoryStorage) Restore(src io.Reader) error {
	return t.cache.Load(src)
}

func (t* inmemoryStorage) DropAll() error {
	t.cache.Flush()
	return nil
}

func (t* inmemoryStorage) DropWithPrefix(prefix []byte) error {

	prefixStr := string(prefix)

	for key, _ := range t.cache.Items() {

		if strings.HasPrefix(key, prefixStr){
			t.cache.Delete(key)
		}

	}

	return nil

}

func (t* inmemoryStorage) Instance() interface{} {
	return t.cache
}
