package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	value   string
	key     string
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, key: lockname, value: kvtest.RandValue(8), version: 0}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey || value == "unlocked" {
			err := lk.ck.Put(lk.key, lk.value, version)

			if err == rpc.OK {
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func (lk *Lock) Release() {
	// Your code here

	value, version, err := lk.ck.Get(lk.key)

	if err == rpc.ErrNoKey {
		return
	}

	if value != lk.value {
		return
	}

	lk.ck.Put(lk.key, "unlocked", version)

}
