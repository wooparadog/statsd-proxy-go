package consistenthash

import (
	"hash/crc32"
	"sort"
)

type Hash func(data []byte) uint32

type Map struct {
	hash    Hash
	keys    []int
	hashMap map[int]string
}

func New(fn Hash) *Map {
	m := &Map{
		hash:    fn,
		hashMap: make(map[int]string),
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

func (m *Map) Populate(keys ...string) {
	m.keys, m.hashMap = m.CalculateHash(keys...)
}

func (m *Map) Get(key []byte) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hash(key))

	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}
