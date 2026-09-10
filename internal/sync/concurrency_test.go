package sync

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartBoundedDoesNotCreateWaitingGoroutines(t *testing.T) {
	s := &Session{indexSem: make(chan struct{}, maxConcurrentIndexJobs)}
	release := make(chan struct{})
	started := make(chan struct{}, maxConcurrentIndexJobs)
	for i := 0; i < maxConcurrentIndexJobs; i++ {
		if !s.startBounded(s.indexSem, func() {
			started <- struct{}{}
			<-release
		}) {
			t.Fatalf("worker %d was rejected before reaching the limit", i)
		}
	}
	for i := 0; i < maxConcurrentIndexJobs; i++ {
		<-started
	}

	const excess = 10_000
	var accidentallyRan int
	var mu sync.Mutex
	for i := 0; i < excess; i++ {
		if s.startBounded(s.indexSem, func() {
			mu.Lock()
			accidentallyRan++
			mu.Unlock()
		}) {
			t.Fatalf("excess job %d was accepted", i)
		}
	}
	close(release)
	s.wg.Wait()
	if accidentallyRan != 0 {
		t.Fatalf("%d rejected jobs still created goroutines", accidentallyRan)
	}
}

func TestIndexNotificationsCoalesceToLatestPendingWork(t *testing.T) {
	s := NewSession(nil, nil, "self", "peer", nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.wg.Add(maxConcurrentIndexJobs)
	for range maxConcurrentIndexJobs {
		go s.runIndexWorker()
	}
	defer func() {
		cancel()
		s.wg.Wait()
	}()

	started := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	var latest atomic.Int32
	if !s.enqueueIndex("reconcile\x00share", func() {
		runs.Add(1)
		close(started)
		<-release
	}) {
		t.Fatal("initial index work was rejected")
	}
	<-started
	for i := int32(1); i <= 100; i++ {
		i := i
		if !s.enqueueIndex("reconcile\x00share", func() {
			runs.Add(1)
			latest.Store(i)
		}) {
			t.Fatalf("redundant index work %d was rejected", i)
		}
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for latest.Load() != 100 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if latest.Load() != 100 || runs.Load() != 2 {
		t.Fatalf("coalesced work ran %d times and retained %d, want 2 and 100", runs.Load(), latest.Load())
	}
	if got := s.Stats().ReconciliationsCoalesced; got != 99 {
		t.Fatalf("coalesced counter = %d, want 99", got)
	}
}
