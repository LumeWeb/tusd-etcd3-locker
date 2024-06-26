package etcd3locker

import (
	"context"
	"os"
	"testing"
	"time"

	etcd_harness "github.com/chen-anders/go-etcd-harness"
	"go.etcd.io/etcd/client/v3"

	"github.com/stretchr/testify/assert"
	"github.com/tus/tusd/v2/pkg/handler"
)

var _ handler.Locker = &Etcd3Locker{}

func TestEtcd3Locker(t *testing.T) {
	a := assert.New(t)

	harness, err := etcd_harness.New(os.Stderr)
	if err != nil {
		t.Fatalf("failed starting etcd harness: %v", err)
	}
	t.Logf("will use etcd harness endpoint: %v", harness.Endpoint)
	defer func() {
		harness.Stop()
		t.Logf("cleaned up etcd harness")
	}()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{harness.Endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Unable to connect to etcd3: %v", err)
	}
	defer func(client *clientv3.Client) {
		err := client.Close()
		if err != nil {
			t.Fatalf("Unable to close etcd3 client: %v", err)
		}
	}(client)

	shortTTL := 3
	testPrefix := "/test-tusd"
	ctx := context.Background()
	unlock := func() {}

	lockerOptions := NewLockerOptions(shortTTL, testPrefix)
	locker, err := NewWithLockerOptions(client, lockerOptions)
	a.NoError(err)

	lock1, err := locker.NewLock("one")
	a.NoError(err)
	a.NoError(lock1.Lock(ctx, unlock))

	//a.Equal(handler.ErrFileLocked, lock1.Lock())
	time.Sleep(5 * time.Second)
	// test that we can't take over the upload via a different etcd3 session
	// while an upload is already taking place; testing etcd3 session KeepAlive
	lock2, err := locker.NewLock("one")
	a.NoError(err)
	a.Equal(handler.ErrFileLocked, lock2.Lock(ctx, unlock))
	a.NoError(lock1.Unlock())
	a.Equal(ErrLockNotHeld, lock1.Unlock())

	testPrefix = "/test-tusd2"
	locker2, err := NewWithPrefix(client, testPrefix)
	a.NoError(err)

	lock3, err := locker2.NewLock("one")
	a.NoError(err)

	a.NoError(lock3.Lock(ctx, unlock))
	a.Equal(handler.ErrFileLocked, lock3.Lock(ctx, unlock))
	a.Equal(handler.ErrFileLocked, lock3.Lock(ctx, unlock))
	a.NoError(lock3.Unlock())
	a.Equal(ErrLockNotHeld, lock3.Unlock())
}
