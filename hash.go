package cuckoo

import (
	"hash/fnv"
)

type hashValue struct {
	hash    uint64
	partial uint8
}

func hashedKey(k string) *hashValue {
	h := fnv.New64()
	h.Write([]byte(k))
	hv := new(hashValue)
	hv.hash = h.Sum64()
	hv.partial = partialKey(hv.hash)
	return hv
}

// partialKey return the upper sizeof(partial_t) bytes of the hashed key.
// It must only depend on the hash value. It cannot change with the hashpower,
// because , in order for `cuckoo_fast_double` to work properly, the alt_index must only
// grow by one bit at the top each time we expand the table.
func partialKey(h uint64) uint8 {
	hash64Bit := h
	hash32Bit := uint32(hash64Bit) ^ uint32(hash64Bit>>32)
	hash16Bit := uint16(hash32Bit) ^ uint16(hash32Bit>>16)
	hash8Bit := uint8(hash16Bit) ^ uint8(hash16Bit>>8)
	return hash8Bit
}
