package cuckoo

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// A fast, lightweight spinlock
type spinLock struct {
	lock_         uint32
	elem_counter_ uint64
}

func (sl *spinLock) Lock() {
	for !atomic.CompareAndSwapUint32(&sl.lock_, 0, 1) {
		runtime.Gosched() // without this it locks up on GOMAXPROCS > 1
	}
}

func (sl *spinLock) Unlock() {
	atomic.StoreUint32(&sl.lock_, 0)
}

func (sl *spinLock) TryLock() bool {
	return !atomic.CompareAndSwapUint32(&sl.lock_, 0, 1)
}

func (sl *spinLock) elemCounter() uint64 {
	return sl.elem_counter_
}

func SpinLock() sync.Locker {
	return &spinLock{}
}
