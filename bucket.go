package cuckoo

import "github.com/willf/bitset"

type bucketContainer struct {
	_buckets []*bucket
	_size    uint64
}

func NewBucketContainer(size uint64) *bucketContainer {
	bc := new(bucketContainer)
	bc._size = size
	bc._buckets = make([]*bucket, size)
	for i := uint64(0); i < size; i++ {
		bc._buckets[i] = &bucket{}
	}
	return bc
}

func (bc *bucketContainer) size() uint64 {
	return bc._size
}

func (bc *bucketContainer) swap(other *bucketContainer) {
	bc._buckets, other._buckets = other._buckets, bc._buckets
	bc._size, other._size = other._size, bc._size
}

func (bc *bucketContainer) bucket(idx uint32) *bucket {
	return bc._buckets[idx]
}

const SlotPerBucket = 128

// The bucket type holds slot_per_bucket() partial keys, key-value pairs, and a
// occupied bitset, which indicates whether the slot at the given bit index is
// in the table or not.
type bucket struct {
	_partials [SlotPerBucket]interface{}
	_occupied *bitset.BitSet
	_kvpairs  [SlotPerBucket]*kvpair
}

func NewBucket() {
	b := new(bucket)
	b._occupied = bitset.New(SlotPerBucket)
}

func (b *bucket) setKV(idx uint, p interface{}, k string, v interface{}) {
	b._partials[idx] = p
	b._occupied.Set(idx)
	b._kvpairs[idx] = &kvpair{k, v}
}

// setKV without copying
func (b *bucket) setKV2(idx uint, p interface{}, kv *kvpair) {
	b._partials[idx] = p
	b._occupied.Set(idx)
	b._kvpairs[idx] = kv
}

func (b *bucket) eraseKV(idx uint) {
	b._partials[idx] = nil
	b._kvpairs[idx] = nil
	b._occupied.Clear(idx)
}

func (b *bucket) kvpair(idx uint) *kvpair {
	return b._kvpairs[idx]
}

func (b *bucket) partial(idx uint) interface{} {
	return b._partials[idx]
}

func (b *bucket) occupied(idx uint) bool {
	return b._occupied.Test(idx)
}

func (b *bucket) key(idx uint) string {
	return b._kvpairs[idx].Key
}

func (b *bucket) value(idx uint) interface{} {
	return b._kvpairs[idx].Value
}

func (b *bucket) clear() {
	for i := 0; i < SlotPerBucket; i++ {
		b.eraseKV(uint(i))
	}
}

// Move the item in b1[slot1] into b2[slot2] without copying
func moveToBucket(b1 *bucket, slot1 uint, b2 *bucket, slot2 uint) {
	tomove := b1.kvpair(slot1)
	b2.setKV2(slot2, b1.partial(slot1), tomove)
	b1.eraseKV(slot1)
}

// Move the contents of b1 to b2
func moveBucket(b1 *bucket, b2 *bucket) {
	var i uint
	for i = 0; i < SlotPerBucket; i++ {
		if b1.occupied(i) {
			moveToBucket(b1, i, b2, i)
		}
	}
}

type kvpair struct {
	Key   string
	Value interface{}
}
