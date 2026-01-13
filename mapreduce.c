#include "mapreduce.h"
#include "threadpool.h"

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

// Key-value pair structure
typedef struct KVPair {
    char *key;
    char *value;
    struct KVPair *next;
} KVPair;

// Partition structure
typedef struct {
    KVPair *head;
    pthread_mutex_t lock;
    size_t bytes;
} Partition;

// Arguments for reduce jobs (partition index and reducer function)
typedef struct {
    unsigned int partition_idx;
    Reducer reducer_fn;
} ReduceArgs;

// File info for sorting map jobs by size
typedef struct {
    char *name;
    size_t size;
} FileInfo;

// Partition info for sorting reduce jobs by bytes
typedef struct {
    unsigned int idx;
    size_t bytes;
} PartInfo;

// Global variables
static Partition *partitions = NULL;
static unsigned int num_partitions = 0;
static ThreadPool_t *pool = NULL;
static Mapper map_func = NULL;

// Hash key to determine partition index
unsigned int MR_Partitioner(char *key, unsigned int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0') {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash % num_partitions;
}

// Insert key-value pair into partition in ascending order by key
static void insert_sorted(Partition *partition, KVPair *pair) {
    if (partition->head == NULL || strcmp(partition->head->key, pair->key) >= 0) {
        pair->next = partition->head;
        partition->head = pair;
    } else {
        KVPair *curr = partition->head;
        
        // Locate the node before the point of insertion
        while (curr->next != NULL && strcmp(curr->next->key, pair->key) < 0) {
            curr = curr->next;
        }
        
        pair->next = curr->next;
        curr->next = pair;
    }
}

// Emit a key-value pair to appropriate partition
void MR_Emit(char *key, char *value) {
    if (!key || !value || num_partitions == 0) return;
    unsigned int idx = MR_Partitioner(key, num_partitions);
    Partition *partition = &partitions[idx];

    char *key_copy = strdup(key);
    char *val_copy = strdup(value);

    KVPair *pair = malloc(sizeof(KVPair));
    pair->key = key_copy;
    pair->value = val_copy;
    pair->next = NULL;
    
    // lock the partition to avoid race conditions among mapper threads
    pthread_mutex_lock(&partition->lock);
    insert_sorted(partition, pair);
    partition->bytes += strlen(key_copy) + strlen(val_copy) + 2;
    pthread_mutex_unlock(&partition->lock);
}



// Map job wrapper function that runs in a pool worker
static void map_wrapper(void *arg) {
    char *filename = (char *)arg;
    map_func(filename);
}

// Comparison function for sorting files by size
int compare_file_size(const void *a, const void *b) {
    FileInfo *fa = (FileInfo *)a;
    FileInfo *fb = (FileInfo *)b;
    if (fa->size > fb->size) return 1;
    if (fa->size < fb->size) return -1;
    return 0;
}

// Comparison function for sorting partitions by bytes
int compare_part_bytes(const void *a, const void *b) {
    PartInfo *pa = (PartInfo*)a;
    PartInfo *pb = (PartInfo*)b;
    if (pa->bytes > pb->bytes) return 1;
    if (pa->bytes < pb->bytes) return -1;
    return 0;
}

// Get next value for a given key in partition
char *MR_GetNext(char *key, unsigned int partition_idx) {
    if (!key || partition_idx >= num_partitions) {
        return NULL;
    }

    Partition *partition = &partitions[partition_idx];
    KVPair *pair = partition->head;

    if (!pair || strcmp(pair->key, key) != 0) {
        return NULL;
    }

    partition->head = pair->next;
    char *value = pair->value;
    free(pair->key);
    free(pair);

    return value;
}

// Reduce job function
// one reducer per partition that runs in a reducer thread
void MR_Reduce(void *arg) {
    ReduceArgs *reduce_args = (ReduceArgs *)arg;
    unsigned int idx = reduce_args->partition_idx;
    Reducer reduce_fn = reduce_args->reducer_fn;
    free(reduce_args);

    Partition *partition = &partitions[idx];

    while (partition->head) {
        char *key = strdup(partition->head->key);
        reduce_fn(key, idx);
        free(key);
    }
}

// Main MapReduce execution function
void MR_Run(unsigned int file_count, char *file_names[],
            Mapper mapper, Reducer reducer,
            unsigned int num_workers, unsigned int num_parts) {
    map_func = mapper;
    num_partitions = num_parts;

    partitions = malloc(num_parts * sizeof(Partition));

    for (unsigned int i = 0; i < num_parts; i++) {
        partitions[i].head = NULL;
        partitions[i].bytes = 0;
        pthread_mutex_init(&partitions[i].lock, NULL);
    }

    pool = ThreadPool_create(num_workers);

    // Map Phase: presort files by size and submit map jobs to thread pool
    FileInfo *files = malloc(file_count * sizeof(FileInfo));

    for (unsigned int i = 0; i < file_count; i++) {
        struct stat st;
        size_t sz = 0;
        if (stat(file_names[i], &st) == 0) {
            sz = (size_t)st.st_size;
        }
        files[i].name = file_names[i];
        files[i].size = sz;
    }
    
    qsort(files, file_count, sizeof(FileInfo), compare_file_size);
    

    for (unsigned int i = 0; i < file_count; i++) {
        ThreadPool_add_job(pool, map_wrapper, files[i].name, files[i].size);
    }

    free(files);

    // Wait for all map jobs to complete
    ThreadPool_check(pool);

    // Reduce Phase: presort partitions by bytes and submit reduce jobs to thread pool
    PartInfo *plist = malloc(num_parts * sizeof(PartInfo));

    for (unsigned int i = 0; i < num_parts; i++) {
        plist[i].idx = i;
        plist[i].bytes = partitions[i].bytes;
    }
    
    qsort(plist, num_parts, sizeof(PartInfo), compare_part_bytes);
    
    // Submit reduce jobs in sorted order
    for (unsigned int k = 0; k < num_parts; k++) {
        unsigned int idx = plist[k].idx;
        ReduceArgs *ra = malloc(sizeof(*ra));
        if (!ra) continue;
        ra->partition_idx = idx;
        ra->reducer_fn = reducer;
        ThreadPool_add_job(pool, MR_Reduce, ra, partitions[idx].bytes);
    }

    free(plist);

    // Wait for all reduce jobs to complete
    ThreadPool_check(pool);

    // Cleanup
    ThreadPool_destroy(pool);

    for (unsigned int i = 0; i < num_parts; i++) {
        pthread_mutex_destroy(&partitions[i].lock);
    }

    free(partitions);
}