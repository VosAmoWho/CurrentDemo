package CurrentMap

import "sync/atomic"

type BucketStatus uint8

const (
	BUCKET_STATUS_NORMAL      BucketStatus = 0
	BUCKET_STATUS_UNDERWEIGHT BucketStatus = 1
	BUCKET_STATUS_OVERWEIGHT  BucketStatus = 2
)

type PairRedistributor interface {
	UpdateThreshold(pairTotal uint64, bucketNumber int)

	CheckBucketStatus(pairTotal uint64, bucketSize uint64) (bucketStatus BucketStatus)

	Redistributor(bucketStatus BucketStatus, bucket []Bucket) (newBucket []Bucket, changed bool)
}

type myRedistributor struct {
	loadFactor            float64
	upperThreshold        uint64
	overweightBucketCount uint64
	emptyBucketCount      uint64
}

func newDefaultPairRedistributor(loadFactor float64, bucketNumber int) PairRedistributor {
	if loadFactor <= 0 {
		loadFactor = DEFAULT_BUCKET_LOAD_FACTOR
	}

	pr := &myRedistributor{}
	pr.loadFactor = loadFactor
	return pr
}

var bucketCountTemplate = `Bucket count: 
    pairTotal: %d
    bucketNumber: %d
    average: %f
    upperThreshold: %d
    emptyBucketCount: %d

`

func (r *myRedistributor) UpdateThreshold(pairTotal uint64, bucketNumber int) {
	var average float64
	average = float64(pairTotal / uint64(bucketNumber))
	if average < 100 {
		average = 100
	}
	atomic.StoreUint64(&r.upperThreshold, uint64(average*r.loadFactor))
}

var bucketStatusTemplate = `Check bucket status: 
    pairTotal: %d
    bucketSize: %d
    upperThreshold: %d
    overweightBucketCount: %d
    emptyBucketCount: %d
    bucketStatus: %d
	
`

func (r *myRedistributor) CheckBucketStatus(pairTotal uint64, bucketSize uint64) (bucketStatus BucketStatus) {
	if bucketSize > DEFAULT_BUCKET_MAX_SIZE || bucketSize > atomic.LoadUint64(&r.upperThreshold) {
		atomic.AddUint64(&r.overweightBucketCount, 1)
		bucketStatus = BUCKET_STATUS_OVERWEIGHT
		return
	}
	if bucketSize == 0 {
		atomic.AddUint64(&r.emptyBucketCount, 1)
	}
	return
}

var redistributionTemplate = `Redistributing: 
    bucketStatus: %d
    currentNumber: %d
    newNumber: %d

`

func (r *myRedistributor) Redistributor(bucketStatus BucketStatus, bucket []Bucket) (newBucket []Bucket, changed bool) {
	currentNumber := uint64(len(bucket))
	newNumber := currentNumber

	switch bucketStatus {
	case BUCKET_STATUS_OVERWEIGHT:
		if atomic.LoadUint64(&r.overweightBucketCount)*4 < currentNumber {
			return nil, false
		}
		newNumber = currentNumber << 1
	case BUCKET_STATUS_UNDERWEIGHT:
		if currentNumber < 100 || atomic.LoadUint64(&r.emptyBucketCount)*4 < currentNumber {
			return nil, false
		}
		newNumber = currentNumber >> 1
		if newNumber < 2 {
			newNumber = 2
		}
	default:
		return nil, false
	}

	if newNumber == currentNumber {
		atomic.StoreUint64(&r.overweightBucketCount, 0)
		atomic.StoreUint64(&r.emptyBucketCount, 0)
		return nil, false
	}

	var pairs []Pair
	for _, b := range bucket {
		for e := b.GetFirstPair(); e != nil; e = e.Next() {
			pairs = append(pairs, e)
		}
	}

	if newNumber > currentNumber {
		for i := uint64(0); i < currentNumber; i++ {
			bucket[i].Clear(nil)
		}

		for j := newNumber - currentNumber; j > 0; j-- {
			bucket = append(bucket, NewBucket())
		}
	} else {
		bucket = make([]Bucket, newNumber)
		for i := uint64(0); i < newNumber; i++ {
			bucket[i] = NewBucket()
		}
	}

	var count int
	for _, p := range pairs {
		index := int(p.Hash() % newNumber)
		b := bucket[index]
		b.Put(p, nil)
		count++
	}

	atomic.StoreUint64(&pr.overweightBucketCount, 0)
	atomic.StoreUint64(&pr.emptyBucketCount, 0)
	return bucket, true
}
