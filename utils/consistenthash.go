package consistenthash

import (
	lru "github.com/hashicorp/golang-lru"
	"hash/crc32"
	"log"
	"sort"
)

type Hash func(data []byte) uint32

type Map struct {
	hash      Hash
	keys      []int
	hashMap   map[int]string
	lru_cache *lru.Cache
}

func New(fn Hash) *Map {
	lru_cache, err := lru.New(50000)
	if err != nil {
		log.Fatal("Lru cache failed")
	}
	m := &Map{
		hash:      fn,
		hashMap:   make(map[int]string),
		lru_cache: lru_cache,
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

func (m *Map) CalculateHash(keys ...string) ([]int, map[int]string) {
	hashed_keys := make([]int, len(keys))
	hash_map := make(map[int]string)

	for i, key := range keys {
		hash := int(m.hash([]byte(key)))
		hashed_keys[i] = hash
		hash_map[hash] = key
	}

	sort.Ints(hashed_keys)
	return hashed_keys, hash_map
}

func (m *Map) Members() []string {
	var r []string
	for _, v := range m.hashMap {
		r = append(r, v)
	}
	return r
}

func (m *Map) Populate(keys ...string) {
	m.keys, m.hashMap = m.CalculateHash(keys...)
	m.lru_cache.Purge()
}

func (m *Map) Get(key []byte) string {
	//lru_key := string(key)
	//result, ok := m.lru_cache.Get(lru_key)
	//if ok {
	//	return result.(string)
	//}
	//log.Println("Got new metrics not in cache: ", lru_key)
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hash(key))

	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	if idx == len(m.keys) {
		idx = 0
	}

	r := m.hashMap[m.keys[idx]]
	//m.lru_cache.Add(lru_key, r)
	return r
}
