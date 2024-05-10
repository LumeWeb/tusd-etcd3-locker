// Package etcd3locker provides a locking mechanism using an etcd3 cluster.
// Tested on etcd 3.1/3.2./3.3
package etcd3locker

import (
	"context"
	"time"

	"github.com/tus/tusd/v2/pkg/handler"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var _ handler.Lock = (*etcd3Lock)(nil)

type etcd3Lock struct {
	Id      string
	Mutex   *concurrency.Mutex
	Session *concurrency.Session

	isHeld bool
}

func newEtcd3Lock(session *concurrency.Session, id string) *etcd3Lock {
	return &etcd3Lock{
		Mutex:   concurrency.NewMutex(session, id),
		Session: session,
	}
}

func (lock *etcd3Lock) Lock(ctx context.Context, requestUnlock func()) error {
	if lock.isHeld {
		// If the lock is already held, invoke the requestUnlock callback
		go requestUnlock()

		// Wait for the context to be cancelled
		select {
		case <-ctx.Done():
			return handler.ErrLockTimeout
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attempt to acquire the lock
	if err := lock.Mutex.Lock(ctx); err != nil {
		if err == context.DeadlineExceeded {
			return handler.ErrFileLocked
		} else {
			return err
		}
	}

	lock.isHeld = true
	return nil
}

// Releases a lock from etcd3
func (lock *etcd3Lock) Unlock() error {
	if !lock.isHeld {
		return ErrLockNotHeld
	}

	lock.isHeld = false
	defer lock.Session.Close()
	return lock.Mutex.Unlock(context.Background())
}
