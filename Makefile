CFLAGS=-Wall -pthread

all: wordcount

threadpool.o: threadpool.c threadpool.h
	gcc $(CFLAGS) -c threadpool.c

mapreduce.o: mapreduce.c mapreduce.h threadpool.h
	gcc $(CFLAGS) -c mapreduce.c

distwc.o: distwc.c mapreduce.h
	gcc $(CFLAGS) -c distwc.c

wordcount: threadpool.o mapreduce.o distwc.o
	gcc $(CFLAGS) -o wordcount threadpool.o mapreduce.o distwc.o

run: wordcount
	./wordcount testcase/sample*.txt

memcheck: wordcount
	valgrind --tool=memcheck --leak-check=yes ./wordcount testcase/sample*.txt

helgrind: wordcount
	valgrind --tool=helgrind --verbose --fair-sched=yes ./wordcount testcase/sample*.txt

clean:
	rm -f *.o wordcount result-*.txt