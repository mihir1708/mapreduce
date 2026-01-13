# MapReduce Framework in C

A lightweight MapReduce-style framework implemented in C, featuring a thread pool for parallel execution and a sample distributed word count application.

---

## Overview

This project implements a simplified **MapReduce framework** in C to explore parallel data processing at a low level. It provides core abstractions for mapping, reducing, and coordinating work across multiple threads using a custom thread pool.

A sample application demonstrates the framework by performing a distributed-style word count over input data.

The project focuses on concurrency, synchronization, and systems-level design rather than large-scale deployment.

---

## What the Project Does

* Implements a basic MapReduce execution model in C
* Uses a thread pool to parallelize map and reduce tasks
* Manages intermediate key–value data between stages
* Demonstrates usage with a word count program
* Coordinates worker threads using synchronization primitives

---

## Implementation Highlights

* Custom thread pool for task scheduling and execution
* Clear separation between framework logic and application logic
* Modular design with reusable MapReduce components
* Emphasis on correctness and thread safety

---

## Tools Used

* C (POSIX)
* pthreads
* GCC / Make

---

## Files

```
mapreduce.c     # Core MapReduce framework logic
mapreduce.h     # MapReduce interfaces and definitions
threadpool.c    # Thread pool implementation
threadpool.h    # Thread pool interfaces
distwc.c        # Distributed-style word count example
```

---

## Notes

This project is intended as a systems programming exercise to understand how parallel data processing frameworks can be built from first principles. It prioritizes clarity and concurrency concepts over production-scale optimizations.
