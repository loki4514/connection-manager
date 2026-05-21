package worker

import (
	"log"
	"sync"
	"time"
)

type Job struct {
	ClientID  string
	Message   string
	ReplyChan chan string
}

type Priority string

const (
	High Priority = "high"
	Low  Priority = "low"
)

type Pool struct {
	HighPriorityQueue chan Job
	LowPriorityQueue  chan Job
	WorkerCount       int
	wg                sync.WaitGroup
}

func workerProcessing(
	wg *sync.WaitGroup,
	workerID int,
	highJobChan chan Job,
	lowJobChan chan Job,
) {
	defer wg.Done()

	for {
		// Process up to 3 high-priority jobs
		for range 3 {
			select {
			case job, ok := <-highJobChan:
				if !ok {
					return
				}
				processJob(workerID, job)
			default:
				goto waitForWork
			}
		}

		// Process 1 low-priority job
		select {
		case job, ok := <-lowJobChan:
			if !ok {
				return
			}
			processJob(workerID, job)
		default:
		}

	waitForWork:
		select {
		case job, ok := <-highJobChan:
			if !ok {
				return
			}
			processJob(workerID, job)
		case job, ok := <-lowJobChan:
			if !ok {
				return
			}
			processJob(workerID, job)
		}
	}
}

func processJob(workerID int, job Job) {
	time.Sleep(time.Second) // simulate work

	log.Printf("Worker %d processing message from %s: %s", workerID, job.ClientID, job.Message)

	select {
	case job.ReplyChan <- "Processed Message: " + job.Message:
		log.Printf("Worker %d reply sent to client %s", workerID, job.ClientID)
	default:
		log.Printf("Worker %d reply dropped for client %s: queue full", workerID, job.ClientID)
	}
}

func NewWorkerPool(count int) *Pool {
	return &Pool{
		HighPriorityQueue: make(chan Job, 5),
		LowPriorityQueue:  make(chan Job, 5),
		WorkerCount:       count,
	}
}

func (p *Pool) Start() {
	// Boot up the workers and return immediately. Don't block!
	for i := 0; i < p.WorkerCount; i++ {
		p.wg.Add(1)
		// We pass the channel itself, not a single job!
		go workerProcessing(&p.wg, i, p.HighPriorityQueue, p.LowPriorityQueue)
	}
}

// Stop safely attempts to gracefully shut down the worker pool.
func (p *Pool) Stop() {
	// 1. Close the channel so workers know to stop when the queue is empty.
	close(p.LowPriorityQueue)
	close(p.HighPriorityQueue)

	// 2. Wait for all active workers to finish their current jobs.
	p.wg.Wait()
}

func (p *Pool) SubmitJob(job Job, jobType Priority) bool {
	switch jobType {
	case High:
		select {
		case p.HighPriorityQueue <- job:
			return true
		default:
			log.Printf("High priority queue full, dropping job for client %s", job.ClientID)
			return false
		}

	case Low:
		select {
		case p.LowPriorityQueue <- job:
			return true
		default:
			log.Printf("Low priority queue full, dropping job for client %s", job.ClientID)
			return false
		}

	default:
		log.Printf("Invalid job priority: %s", jobType)
		return false
	}
}
