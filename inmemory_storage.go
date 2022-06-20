/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
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

func (t* inmemoryStorage) GetRaw(prefix, key []byte, ttlPtr *int, versionPtr *int64, required bool) ([]byte, error) {
	return t.getImpl(prefix, key, required)
}

func (t* inmemoryStorage) SetRaw(prefix, key, value []byte, ttlSeconds int) error {

	rawKey := append(prefix, key...)

	ttl := cache.NoExpiration
	if ttlSeconds > 0 {
		ttl = time.Second * time.Duration(ttlSeconds)
	}

	t.cache.Set(string(rawKey), value, ttl)
	return nil
}

func (t *inmemoryStorage) DoInTransaction(prefix, key []byte, cb func(entry *storage.RawEntry) bool) error {

	rawKey := append(prefix, key...)

	rawEntry := &storage.RawEntry {
		Key: rawKey,
		Ttl: storage.NoTTL,
		Version: 0,
	}

	if obj, ok := t.cache.Get(string(rawKey)); ok && obj != nil {
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

	t.cache.Set(string(rawKey), rawEntry.Value, ttl)
	return nil
}

func (t* inmemoryStorage) CompareAndSetRaw(bucket, key, value []byte, ttlSeconds int, version int64) (bool, error) {
	return true, t.SetRaw(bucket, key, value, ttlSeconds)
}

func (t* inmemoryStorage) RemoveRaw(prefix, key []byte) error {
	rawKey := append(prefix, key...)
	t.cache.Delete(string(rawKey))
	return nil
}

func (t* inmemoryStorage) getImpl(prefix, key []byte, required bool) ([]byte, error) {

	rawKey := append(prefix, key...)
	var val []byte
	if obj, ok := t.cache.Get(string(rawKey)); ok && obj != nil {
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
				Value:   val,
				Ttl:     int(item.Expiration),
				Version: item.Expiration,
			}
			if !cb(&re) {
				break
			}
		}

	}

	return nil
}

func (t* inmemoryStorage) FetchKeysRaw(prefix []byte, batchSize int) ([][]byte, error) {

	prefixStr := string(prefix)
	var keys [][]byte

	for key, _ := range t.cache.Items() {

		if strings.HasPrefix(key, prefixStr){
			keys = append(keys, []byte(key))
		}

	}

	return keys, nil
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
