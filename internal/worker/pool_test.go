package worker

import (
	"testing"
	"time"
)

func TestNewWorkerPool(t *testing.T) {
	pool := NewWorkerPool(5)
	if pool.WorkerCount != 5 {
		t.Errorf("Expected WorkerCount to be 5, got %d", pool.WorkerCount)
	}
	if pool.HighPriorityQueue == nil || cap(pool.HighPriorityQueue) != 5 {
		t.Errorf("Expected HighPriorityQueue with capacity 5")
	}
	if pool.LowPriorityQueue == nil || cap(pool.LowPriorityQueue) != 5 {
		t.Errorf("Expected LowPriorityQueue with capacity 5")
	}
}

func TestSubmitJob(t *testing.T) {
	pool := NewWorkerPool(1) // Not started

	job := Job{ClientID: "client1", Message: "test"}

	// Fill High priority queue (capacity 5)
	for i := 0; i < 5; i++ {
		if !pool.SubmitJob(job, High) {
			t.Errorf("Expected to successfully submit job %d to High priority queue", i)
		}
	}

	// 6th job should fail
	if pool.SubmitJob(job, High) {
		t.Errorf("Expected failing to submit to full High priority queue")
	}

	// Fill Low priority queue (capacity 5)
	for i := 0; i < 5; i++ {
		if !pool.SubmitJob(job, Low) {
			t.Errorf("Expected to successfully submit job %d to Low priority queue", i)
		}
	}

	// 6th job should fail
	if pool.SubmitJob(job, Low) {
		t.Errorf("Expected failing to submit to full Low priority queue")
	}

	// Invalid priority
	if pool.SubmitJob(job, Priority("invalid")) {
		t.Errorf("Expected failing to submit job with invalid priority")
	}
}

func TestPoolJobProcessing(t *testing.T) {
	pool := NewWorkerPool(2)
	pool.Start()

	replyChan := make(chan string, 10)

	expectedJobs := 0
	submit := func(job Job, p Priority) {
		if pool.SubmitJob(job, p) {
			expectedJobs++
		}
	}

	submit(Job{ClientID: "h1", Message: "hm1", ReplyChan: replyChan}, High)
	submit(Job{ClientID: "h2", Message: "hm2", ReplyChan: replyChan}, High)
	submit(Job{ClientID: "h3", Message: "hm3", ReplyChan: replyChan}, High)
	submit(Job{ClientID: "l1", Message: "lm1", ReplyChan: replyChan}, Low)
	submit(Job{ClientID: "h4", Message: "hm4", ReplyChan: replyChan}, High)
	submit(Job{ClientID: "h5", Message: "hm5", ReplyChan: replyChan}, High)
	submit(Job{ClientID: "h6", Message: "hm6", ReplyChan: replyChan}, High)

	repliesReceived := 0
	timeout := time.After(6 * time.Second) // Workers take 1s per job, 7 jobs / 2 workers = ~4s, give it 6s

	for repliesReceived < expectedJobs {
		select {
		case msg := <-replyChan:
			// msg should have prefix "Processed Message:"
			if len(msg) < 18 || msg[:18] != "Processed Message:" {
				t.Errorf("Unexpected reply message: %s", msg)
			}
			repliesReceived++
		case <-timeout:
			t.Fatalf("Timeout waiting for replies, got %d out of %d", repliesReceived, expectedJobs)
		}
	}

	pool.Stop()
}

func TestProcessJob_DroppedReply(t *testing.T) {
	// Unbuffered channel with no reader should cause the default case in select to execute
	replyChan := make(chan string)
	job := Job{ClientID: "client1", Message: "test", ReplyChan: replyChan}

	done := make(chan struct{})
	go func() {
		processJob(1, job)
		close(done)
	}()

	select {
	case <-done:
		// test passed, it didn't block
	case <-time.After(2 * time.Second):
		t.Fatal("processJob blocked when trying to send reply to a full/unbuffered channel")
	}
}
