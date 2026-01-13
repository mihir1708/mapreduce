#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "mapreduce.h"

void Map(char* file_name) {
    FILE* fp = fopen(file_name, "r");
    assert(fp != NULL);

    char* line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(char* key, unsigned int partition_idx) {
    int count = 0;
    char *value, name[100];
    while ((value = MR_GetNext(key, partition_idx)) != NULL) {
        count++;
        free(value);
    }
    sprintf(name, "result-%d.txt", partition_idx);
    FILE* fp = fopen(name, "a");
    fprintf(fp, "%s: %d\n", key, count);
    fclose(fp);
}

int main(int argc, char *argv[]) {
    // struct timeval start, end;
    // gettimeofday(&start, NULL);
    
    MR_Run(argc - 1, &(argv[1]), Map, Reduce, 5, 10);
    
    // gettimeofday(&end, NULL);
    // double time_taken;
    // time_taken = (end.tv_sec - start.tv_sec) * 1e6;
    // time_taken = (time_taken + (end.tv_usec - start.tv_usec)) * 1e-6;
    // printf("Time taken: %f seconds\n", time_taken);
}