package cuckoo

// A fixed-size array, broken up into segments that are dynamically allocated upon request.
// It is the user's reponsibility to make sure they only access allocated parts of the array.
// High                          Low
// +---------------+ +--------------+
// |  SegmentBits  | |  OffsetBits  |
// +---------------+ +--------------+

type lazyArray struct {
	_segments          [][]*spinLock
	_allocatedSegments uint64

	// the number of bits of the index used as the offset within a segment
	_offsetBits uint8

	// the number of bits of the index used as the segment index
	_segmentBits uint8

	_segmentSize uint64
	_numSegments uint64
	_offsetMask  uint64
}

func NewLazyArray(offsetBits uint8, segmentBits uint8, target uint64) *lazyArray {
	la := new(lazyArray)
	la._offsetBits = offsetBits
	la._segmentBits = segmentBits
	la._segmentSize = 1 << offsetBits
	la._numSegments = 1 << segmentBits
	la._offsetMask = la._segmentSize - 1
	la.resize(target)
	return la
}

// Returns the number of elements the array has allocated space for
func (la *lazyArray) size() uint64 {
	return uint64(la._allocatedSegments) * la._segmentSize
}

func (la *lazyArray) maxSize() uint64 {
	return 1 << (la._offsetBits + la._segmentBits)
}

func (la *lazyArray) getSegment(i uint64) uint64 {
	return i >> la._offsetBits
}

func (la *lazyArray) getOffset(i uint64) uint64 {
	return i & la._offsetMask
}

// get return a reference to the data at the given index
func (la *lazyArray) get(idx uint64) *spinLock {
	return la._segments[la.getSegment(idx)][la.getOffset(idx)]
}

// resize allocate enough space for target elements, not excedding the capacity of the array.
// Under no circumstance will the array be shrunk.
func (la *lazyArray) resize(target uint64) {
	la._segments = make([][]*spinLock, 0, la._numSegments)
	_maxSize := la.maxSize()
	if _maxSize < target {
		target = _maxSize
	}
	if target == 0 {
		return
	}
	lastSegment := la.getSegment(target - 1)
	for i := la._allocatedSegments; i <= lastSegment; i++ {
		la._segments[i] = la.createArray()
	}
	la._allocatedSegments = lastSegment + 1
}

func (la *lazyArray) createArray() []*spinLock {
	return make([]*spinLock, la._segmentSize)
}
