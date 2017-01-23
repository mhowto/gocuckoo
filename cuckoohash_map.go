package cuckoo

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	// The default minimum load factor that the table allows for automatic expansion.
	// It must be a number between 0.0 and 1.0.
	CUCKOO_DEFAULT_MINIMUM_LOAD_FACTOR float64 = 0.05
	CUCKOO_NO_MAXIMUM_HASHPOSER                = 0
)

type HashFn func(string) uint64
type EqualFn func(string) bool

type CuckooHashMap struct {
	// 2**_hashPower is the number of buckets.
	_hashPower uint32

	// The hash function
	_hashFn HashFn

	// The equality function
	_equalFn EqualFn

	// vector of buckets. The size or memory location of the buckets cannot be changed unless
	// all the locks are taken on the table. Thus, it is only safe to access the _buckets vector
	// when you have at least one lock held.
	_buckets *bucketContainer

	// array of locks. They are mutable, so that const methods can take locks.
	// Even though it's a vector, it should not ever change in size after the initial
	// allocation.
	_locks *lazyArray

	// a lock to synchronize expansions
	_expansionLock sync.Mutex

	// stores the minimum load factor(float64) allowed for automatic expansions.
	_minLoadFactor atomic.Value

	// stores the maximum hash power(float64) allowed for any expansions.
	_maxHashPower uint32
}

func NewCuckooHashMap(power uint32, hf HashFn, eql EqualFn) *CuckooHashMap {
	// 0 <= power <= 16
	m := new(CuckooHashMap)
	m._hashPower = power
	m._hashFn = hf
	m._equalFn = eql
	m._minLoadFactor.Store(CUCKOO_DEFAULT_MINIMUM_LOAD_FACTOR)
	m._maxHashPower = CUCKOO_NO_MAXIMUM_HASHPOSER
	target := HashSize(power)
	m._buckets = NewBucketContainer(target)
	m._locks = NewLazyArray(16, 0, target)

	return m
}

// reserverCalc takes in a parameter specifying a certain number of slots
// for a table and returns the smallest hashpower that will hold n elements.
func reserveCalc(n uint64) uint32 {
	buckets := (n + SlotPerBucket - 1) / SlotPerBucket
	var blog2 uint32
	for blog2 = 1; (1 << blog2) < buckets; blog2++ {
	}
	if n > HashSize(blog2)*SlotPerBucket {
		panic("n > hashsize(blog2) * SlotPerBucket")
	}

	return blog2
}

// HashSize returns the number of buckets corresponding to a given hashpower
func HashSize(hp uint32) uint64 {
	return 1 << hp
}

// HashMask returns the bitmask for the buckets array corresponding to a given hashpower.
func HashMask(hp uint32) uint64 {
	return HashSize(hp) - 1
}

func (m *CuckooHashMap) HashPower() uint32 {
	return m._hashPower
}

func (m *CuckooHashMap) BucketCount() uint64 {
	return m._buckets.size()
}

func (m *CuckooHashMap) Empty() bool {
	for i := uint64(0); i < m._locks.size(); i++ {
		if m._locks.get(i).elemCounter() > 0 {
			return false
		}
	}
	return true
}

// return the number of elements in the table
func (m *CuckooHashMap) Size() uint64 {
	var s uint64
	for i := uint64(0); i < m._locks.size(); i++ {
		s += m._locks.get(i).elemCounter()
	}
	return s
}

func (m *CuckooHashMap) Capacity() uint64 {
	return m.BucketCount() * SlotPerBucket
}

func (m *CuckooHashMap) LoadFactor() float64 {
	return float64(m.Size()) / float64(m.Capacity())
}

func (m *CuckooHashMap) SetMinimumLoadFactor(mlf float64) error {
	if mlf < 0.0 {
		return errors.New("load factor cannot be less than 0")
	} else if mlf > 1.0 {
		return errors.New("load factor cannot be greater than 1")
	}
	m._minLoadFactor.Store(mlf)
	return nil
}

func (m *CuckooHashMap) MinimumLoadFactor() float64 {
	mlf := m._minLoadFactor.Load()
	return mlf.(float64)
}

func (m *CuckooHashMap) SetMaxmumHashPower(mhp uint32) error {
	if mhp != CUCKOO_NO_MAXIMUM_HASHPOSER && m.HashPower() > mhp {
		return errors.New("maximum hashpower is less than current hashpower")
	}
	atomic.StoreUint32(&m._maxHashPower, mhp)
	return nil
}

func (m *CuckooHashMap) MaxmumHashPower() uint32 {
	return m._maxHashPower
}

// Upsert searches for key in the table. If the key is not there, it is inserted
// with val. If the key is there, then fn is called on the value. The key
// will be immediately constructed as key_type. If the insertion succeeds,
// this constructed key will be moved into the table and the value constructed
// from the val parameters. If the insertion fails, the constructed key will be
// destroyed, and the val parameters will remain valid. If there is no room left
// in the table, it will be automatically expanded.
func (m *CuckooHashMap) Upsert(key string, val interface{}) bool {
	/*
		hv := hashedKey(key)
		b := m.snapshotAndLockTwo(hv)
		pos := m.cuckooInsertLoop(hv, b, key)
		if pos.status != ok {
			return false
		}
		m.addToBucket(pos.index, pos.slot, hv.partial, key, val)
	*/
	return true
}

// snapshotAndLockTwo loads locks the buckets associated with the given hash value,
// making sure the hashpower doesn't change before the locks are taken. Thus it ensures
// that the buckets and locks corresponding to the hash value will stay correct as long
// as the locks are held. It returns the bucket indices associated with the hash value
// and the current hashpower.
func (m *CuckooHashMap) snapshotAndLockTwo(hv *hashValue) *twoBuckets {
	for {
		// Store the current hashpower we're using to compute the buckets
		hp := m.HashPower()
		i1 := indexHash(hp, hv.hash)
		i2 := altIndex(hp, hv.partial, i1)
		tb, err := m.lockTwo(hp, i1, i2)
		if err == nil {
			return tb
		}
	}
}

type tablePosition struct {
	index uint64
	slot  uint64
}

// cuckooInsertLoop runs cuckoo_insert in a loop until it succeeds in insert and upsert, so
// we pulled out the loop to avoid duplicating logic.
//
//
func (m *CuckooHashMap) cuckooInsertLoop(hv *hashValue, b *twoBuckets, k string) *tablePosition {
	return nil
}

// cuckooInsert tries to find an empty slot in either of the buckets to insert the given key
// into, performing cuckoo hashing if necessary. It expectes the locks to be taken outside the
// function. Before inserting, it checks that the key isn't already in the table. cuckoo hashing
// presents multiple concurrency issues, which are explained in the function. The following return
// states are possible:
//
// ok --
//
// failure_key_duplicated --
//
// failure_under_expansion --
//
// failure_table_full --
//
func (m *CuckooHashMap) cuckooInsert() {

}

// lockTwo locks the two bucket indexes, always locking the earlier index first to avoid
// deadlock. If the two indexes are the same, it just locks one.
func (m *CuckooHashMap) lockTwo(hp uint32, idx1 uint64, idx2 uint64) (*twoBuckets, error) {
	l1 := m.lockIndex(idx1)
	l2 := m.lockIndex(idx2)
	if l2 < l1 {
		l1, l2 = l2, l1
	}
	m._locks.get(l1).Lock()
	m.checkHashPower(hp, l1)
	if l2 != l1 {
		m._locks.get(l2).Lock()
	}
	return &twoBuckets{l1, l2, m._locks}, nil
}

// indexHash returns the first possible bucket that the given hashed key could be.
func indexHash(hp uint32, hv uint64) uint64 {
	return hv & HashMask(hp)
}

// altIndex returns the other possible bucket that the given hashed key could be.
// It takes the first possible bucket as a parameter. Note that this function will
// return the first possible bucket if index is the second possible bucket, so
// alt_index(ti, partial, alt_index(ti, partial, index_hash(ti, hv))) === index(ti, hv)
// 两步异或结果相同
func altIndex(hp uint32, partial uint8, idx uint64) uint64 {
	// ensure tag is nonzero for the multiply. 0xc6a4a7935bd1e995 is the
	// hash constant from 64-bit MurmurHash2
	nonzeroTag := uint64(partial + 1)
	return (idx ^ (nonzeroTag * 0xc6a4a7935bd1e995)) & HashMask(hp)
}

// lockIndex converts an index into buckets to an index into locks
func (m *CuckooHashMap) lockIndex(idx uint64) uint64 {
	return idx & (m._locks.maxSize() - 1)
}

func (m *CuckooHashMap) checkHashPower(hp uint32, lockIndex uint64) error {
	if m.HashPower() != hp {
		m._locks.get(lockIndex).Unlock()
		return fmt.Errorf(
			"check hash power failed, HashPower: %d, hp: %d, they are not equal",
			m.HashPower(),
			hp,
		)
	}
	return nil
}

type twoBuckets struct {
	index1 uint64
	index2 uint64
	locks  *lazyArray
}

func newTwoBuckets(i1, i2 uint64) *twoBuckets {
	return &twoBuckets{
		index1: i1,
		index2: i2,
	}
}

func (tb *twoBuckets) first() uint64 {
	return tb.index1
}

func (tb *twoBuckets) second() uint64 {
	return tb.index2
}

func (tb *twoBuckets) isActive() bool {
	return tb.locks != nil
}

func (tb *twoBuckets) unlock() {
	tb.locks = nil
}
