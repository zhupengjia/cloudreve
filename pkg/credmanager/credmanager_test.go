package credmanager

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/pkg/cache"
	"github.com/stretchr/testify/require"
)

type refreshTestCache struct {
	cache.Driver

	mu              sync.Mutex
	value           any
	lockToCheck     *sync.Mutex
	readWithoutLock atomic.Bool
}

func (c *refreshTestCache) Set(_ string, value any, _ int) error {
	c.mu.Lock()
	c.value = value
	c.mu.Unlock()
	return nil
}

func (c *refreshTestCache) Get(_ string) (any, bool) {
	if c.lockToCheck != nil && c.lockToCheck.TryLock() {
		c.readWithoutLock.Store(true)
		c.lockToCheck.Unlock()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value, c.value != nil
}

type refreshTestCredential struct {
	key            string
	expiresAt      time.Time
	refreshes      *atomic.Int32
	refreshStarted chan struct{}
	releaseRefresh chan struct{}
}

func (c refreshTestCredential) String() string {
	return c.key
}

func (c refreshTestCredential) Refresh(context.Context) (Credential, error) {
	if c.refreshes.Add(1) == 1 && c.refreshStarted != nil {
		close(c.refreshStarted)
		<-c.releaseRefresh
	}
	c.expiresAt = time.Now().Add(time.Hour)
	return c, nil
}

func (c refreshTestCredential) Key() string {
	return c.key
}

func (c refreshTestCredential) Expiry() time.Time {
	return c.expiresAt
}

func (c refreshTestCredential) RefreshedAt() *time.Time {
	return nil
}

func TestObtainReadsCredentialUnderKeyLock(t *testing.T) {
	store := &refreshTestCache{}
	manager := New(store).(*credManager)
	credential := refreshTestCredential{key: "test", expiresAt: time.Now().Add(time.Hour)}
	require.NoError(t, manager.Upsert(context.Background(), credential))

	store.lockToCheck = manager.locks[credential.key]
	_, err := manager.Obtain(context.Background(), credential.key)

	require.NoError(t, err)
	require.False(t, store.readWithoutLock.Load())
}

func TestObtainRefreshesExpiredCredentialOnce(t *testing.T) {
	store := &refreshTestCache{}
	manager := New(store)
	var refreshes atomic.Int32
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})

	err := manager.Upsert(context.Background(), refreshTestCredential{
		key:            "test",
		expiresAt:      time.Now().Add(-time.Hour),
		refreshes:      &refreshes,
		refreshStarted: refreshStarted,
		releaseRefresh: releaseRefresh,
	})
	require.NoError(t, err)

	const callers = 8
	start := make(chan struct{})
	errs := make(chan error, callers)
	var entered sync.WaitGroup
	entered.Add(callers)
	for range callers {
		go func() {
			<-start
			entered.Done()
			_, err := manager.Obtain(context.Background(), "test")
			errs <- err
		}()
	}
	close(start)
	entered.Wait()
	<-refreshStarted
	close(releaseRefresh)

	for range callers {
		require.NoError(t, <-errs)
	}
	require.Equal(t, int32(1), refreshes.Load())
}

func TestObtainMissingCredentialDoesNotCreateLock(t *testing.T) {
	manager := New(&refreshTestCache{}).(*credManager)

	_, err := manager.Obtain(context.Background(), "missing")

	require.ErrorIs(t, err, ErrNotFound)
	_, exists := manager.getLock("missing")
	require.False(t, exists)
}
