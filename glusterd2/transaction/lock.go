package transaction

import (
	"context"
	"errors"
	"time"

	"github.com/gluster/glusterd2/glusterd2/store"

	"github.com/coreos/etcd/clientv3/concurrency"
)

const (
	lockPrefix        = "locks/"
	lockObtainTimeout = 5 * time.Second
)

var (
	// ErrLockTimeout is the error returned when lock could not be obtained
	// and the request timed out
	ErrLockTimeout = errors.New("could not obtain lock: another conflicting transaction may be in progress")
	// ErrLockExists is returned when a lock already exists within the transaction
	ErrLockExists = errors.New("existing lock found for given lock ID")
)

func (t *Txn) lock(lockID string) error {
	// Ensure that no prior lock exists for the given lockID in this transaction
	if _, ok := t.locks[lockID]; ok {
		return ErrLockExists
	}

	logger := t.Ctx.Logger().WithField("lockID", lockID)
	logger.Debug("attempting to obtain lock")

	key := lockPrefix + lockID
	locker := concurrency.NewMutex(store.Store.Session, key)

	ctx, cancel := context.WithTimeout(store.Store.Ctx(), lockObtainTimeout)
	defer cancel()

	err := locker.Lock(ctx)
	switch err {
	case nil:
		logger.Debug("lock obtained")
		// Attach lock to the transaction
		t.locks[lockID] = locker

	case context.DeadlineExceeded:
		// Propagate this all the way back to the client as a HTTP 409 response
		logger.Debug("timeout: failed to obtain lock")
		err = ErrLockTimeout

	default:
		logger.WithError(err).Error("failed to obtain lock")
	}

	return err
}

// Lock obtains a cluster wide transaction lock on the given lockID/lockIDs,
// and attaches the obtained locks to the transaction
func (t *Txn) Lock(lockID string, lockIDs ...string) error {
	if err := t.lock(lockID); err != nil {
		return err
	}
	for _, id := range lockIDs {
		if err := t.lock(id); err != nil {
			return err
		}
	}
	return nil
}
