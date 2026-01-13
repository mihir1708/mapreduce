#include "threadpool.h"
#include <pthread.h>
#include <stdlib.h>

// Add job into queue sorted by job_size (SJF)
static void add_job_to_queue(ThreadPool_job_queue_t *q, ThreadPool_job_t *job) {
    if (q->head == NULL || q->head->job_size >= job->job_size) {
        job->next = q->head;
        q->head = job;
    } else {
        ThreadPool_job_t *curr = q->head;
        
        // Locate the node before the point of insertion
        while (curr->next != NULL && curr->next->job_size < job->job_size) {
            curr = curr->next;
        }
        
        job->next = curr->next;
        curr->next = job;
    }
    q->size++;
}

// Create a thread pool
ThreadPool_t *ThreadPool_create(unsigned int num) {
    if (num == 0) return NULL;
    ThreadPool_t *tp = (ThreadPool_t*) malloc(sizeof(ThreadPool_t));
    tp->threads = (pthread_t*) malloc(sizeof(pthread_t) * num);
    tp->num_threads = num;
    tp->active_workers = 0;
    tp->jobs.head = NULL;
    tp->jobs.size = 0;
    tp->stop = false;

    // initialize the mutex and condition variables
    pthread_mutex_init(&tp->lock, NULL);
    pthread_cond_init(&tp->has_job, NULL);
    pthread_cond_init(&tp->all_idle, NULL);

    for (unsigned int i = 0; i < num; i++) {
        pthread_create(&tp->threads[i], NULL, Thread_run, tp);
    }

    return tp;
}

// Clean up thread pool and free all resources
void ThreadPool_destroy(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->lock);
    tp->stop = true;
    pthread_cond_broadcast(&tp->has_job);
    pthread_mutex_unlock(&tp->lock);

    for (unsigned int i = 0; i < tp->num_threads; i++) {
        pthread_join(tp->threads[i], NULL);
    }

    ThreadPool_job_t *job = tp->jobs.head;
    while (job) {
        ThreadPool_job_t *next = job->next;
        free(job);
        job = next;
    }

    pthread_mutex_destroy(&tp->lock);
    pthread_cond_destroy(&tp->has_job);
    pthread_cond_destroy(&tp->all_idle);

    free(tp->threads);
    free(tp);
}

// Add a job to the thread pool
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg, size_t job_size) {
    ThreadPool_job_t *job = (ThreadPool_job_t *) malloc(sizeof(ThreadPool_job_t));
    job->func = func;
    job->arg = arg;
    job->job_size = job_size;
    job->next = NULL;

    pthread_mutex_lock(&tp->lock);
    if (tp->stop){ // don't add new jobs if the thread pool is stopped
        pthread_mutex_unlock(&tp->lock);
        free(job);
        return false;
    }
    add_job_to_queue(&tp->jobs, job);
    pthread_cond_signal(&tp->has_job); // wake up a waiting worker thread
    pthread_mutex_unlock(&tp->lock);

    return true;
}

// Get a job from the thread pool
// Note: Caller must hold the lock on the thread pool before calling this function
ThreadPool_job_t* ThreadPool_get_job(ThreadPool_t* tp) {
    if (tp->jobs.head == NULL) return NULL;
    ThreadPool_job_t *job = tp->jobs.head;
    tp->jobs.head = job->next;
    tp->jobs.size--;
    job->next = NULL;
    return job;
}


// Worker thread continuously waits for jobs, executes them, and signals when idle
void *Thread_run(void *arg) {
    ThreadPool_t *tp = (ThreadPool_t *) arg;

    while (1) {
        pthread_mutex_lock(&tp->lock);
        // wait while there is no work to do and thread pool not stopped
        while (tp->jobs.size == 0 && !tp->stop) {
            pthread_cond_wait(&tp->has_job, &tp->lock);
        }
        // exit if stopping and no work to do
        if (tp->stop && tp->jobs.size == 0) {
            pthread_mutex_unlock(&tp->lock);
            break;
        }

        ThreadPool_job_t *job = ThreadPool_get_job(tp);
        if (!job) {
            pthread_mutex_unlock(&tp->lock);
            continue;
        }

        tp->active_workers++;
        pthread_mutex_unlock(&tp->lock);

        // run job outside the lock
        job->func(job->arg);

        pthread_mutex_lock(&tp->lock);

        tp->active_workers--;

        // signal all_idle if no jobs and no active workers
        if (tp->jobs.size == 0 && tp->active_workers == 0) {
            pthread_cond_broadcast(&tp->all_idle);
        }
        pthread_mutex_unlock(&tp->lock);
        free(job);
    }

    return NULL;
}

// Wait until all jobs are completed and all workers are idle
void ThreadPool_check(ThreadPool_t *tp) {
    pthread_mutex_lock(&tp->lock);
    while (tp->jobs.size > 0 || tp->active_workers > 0) {
        pthread_cond_wait(&tp->all_idle, &tp->lock);
    }
    pthread_mutex_unlock(&tp->lock);
}