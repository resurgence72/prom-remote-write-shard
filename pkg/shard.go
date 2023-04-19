package pkg

import (
	"sort"
	"strconv"
	"sync"

	"github.com/spaolacci/murmur3"
)

type Hash func(data []byte) uint32

type Option func(*Map)

func WithReplicas(replicas int) Option {
	return func(m *Map) {
		m.replicas = replicas
	}
}

func WithCRC32Hash(h Hash) Option {
	return func(m *Map) {
		m.hash = h
	}
}

const defaultReplicas = 500

type Map struct {
	hash     Hash
	replicas int
	keys     []int // sorted
	hashMap  map[int]string

	lock sync.RWMutex
}

// 初始化ring
func New(opts ...Option) *Map {
	m := &Map{
		hashMap: make(map[int]string),
	}

	for _, opt := range opts {
		opt(m)
	}

	if m.hash == nil {
		m.hash = murmur3.Sum32
	}

	if m.replicas <= 0 {
		m.replicas = defaultReplicas
	}

	return m
}

func (m *Map) ReHash(keys ...string) {
	//*m = Map{
	//	hashMap: make(map[int]string),
	//	keys:    make([]int, 0),
	//}
	m.hashMap = make(map[int]string)
	m.keys = make([]int, 0)

	m.Adds(keys...)
}

// 批量添加key到ring
func (m *Map) Adds(keys ...string) {
	for _, key := range keys {
		m.add(key)
	}
	m.sortKeys()
}

// 添加单个key到ring
func (m *Map) Add(key string) {
	m.add(key)
	m.sortKeys()
}

func (m *Map) sortKeys() {
	m.lock.Lock()
	defer m.lock.Unlock()
	sort.Ints(m.keys)
}

func (m *Map) add(key string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = key
	}
}

// 根据key获取node
func (m *Map) Get(key []byte) string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.hash(key))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	return m.hashMap[m.keys[idx%len(m.keys)]]
}
