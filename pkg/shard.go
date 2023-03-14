package pkg

import (
	"sort"
	"strconv"
	"sync"

	"github.com/spaolacci/murmur3"
)

type Hash func(data []byte) uint32

const defaultReplicas = 500

type Map struct {
	hash     Hash
	replicas int
	keys     []int // sorted
	hashMap  map[int]string

	lock sync.RWMutex
}

// 初始化ring
func New(replicas int, fn Hash) *Map {
	m := &Map{
		hash:     fn,
		replicas: replicas,
		hashMap:  make(map[int]string),
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
	m.lock.Lock()
	defer m.lock.Unlock()

	m = &Map{
		hashMap: make(map[int]string),
		keys:    make([]int, 0),
	}

	m.Adds(keys...)
}

// 批量添加key到ring
func (m *Map) Adds(keys ...string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, key := range keys {
		m.add(key)
	}
	sort.Ints(m.keys)
}

// 添加单个key到ring
func (m *Map) Add(key string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.add(key)
	sort.Ints(m.keys)
}

func (m *Map) add(key string) {
	for i := 0; i < m.replicas; i++ {
		hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = key
	}
}

// 根据key获取node
func (m *Map) Get(key string) string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	return m.hashMap[m.keys[idx%len(m.keys)]]
}
