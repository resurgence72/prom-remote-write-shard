package pkg

import (
	"github.com/cespare/xxhash/v2"
)

// See the following docs:
// - https://www.eecs.umich.edu/techreports/cse/96/CSE-TR-316-96.pdf
// - https://github.com/dgryski/go-rendezvous
// - https://dgryski.medium.com/consistent-hashing-algorithmic-tradeoffs-ef6b8e2fcae8
type ConsistentHash struct {
	hashSeed      uint64
	replicaFactor int
	nodeHashes    []uint64
}

func NewConsistentHash(nodes []string, replica int, hashSeed uint64) *ConsistentHash {
	if replica <= 0 {
		replica = 1
	}

	if replica > len(nodes) {
		replica = len(nodes)
	}

	nodeHashes := make([]uint64, len(nodes))
	for i, node := range nodes {
		nodeHashes[i] = xxhash.Sum64([]byte(node))
	}
	return &ConsistentHash{
		hashSeed:      hashSeed,
		nodeHashes:    nodeHashes,
		replicaFactor: replica,
	}
}

func (rh *ConsistentHash) GetReplicaNodeIdx(h uint64, excludeIdxs []int) []int {
	var replicaNodeIdxs []int
	for i := 0; i < rh.replicaFactor; i++ {
		idx := rh.GetNodeIdx(h, excludeIdxs)
		excludeIdxs = append(excludeIdxs, idx)
		replicaNodeIdxs = append(replicaNodeIdxs, idx)
	}
	return replicaNodeIdxs
}

func (rh *ConsistentHash) GetNodeIdx(h uint64, excludeIdxs []int) int {
	var mMax uint64
	var idx int
	h ^= rh.hashSeed
next:
	for i, nh := range rh.nodeHashes {
		for _, j := range excludeIdxs {
			if i == j {
				continue next
			}
		}
		if m := fastHashUint64(nh ^ h); m > mMax {
			mMax = m
			idx = i
		}
	}
	return idx
}

func fastHashUint64(x uint64) uint64 {
	x ^= x >> 12 // a
	x ^= x << 25 // b
	x ^= x >> 27 // c
	return x * 2685821657736338717
}
