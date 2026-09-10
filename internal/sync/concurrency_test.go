package sync

import (
	"sync"
	"testing"
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
