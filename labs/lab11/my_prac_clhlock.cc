#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

// #define NUM_THREAD		16
#define NUM_WORK		1000000

int cnt_global;
/* to allocate cnt_global & object_tas in different cache lines */
int gap[128];
int object_tas;

int* tail;
int* lock() {
	int* myNode = (int*)malloc(sizeof(int));
	*myNode = true;
	int* pred = __sync_lock_test_and_set(&tail, myNode);
	while (pred != NULL && *pred == true) {}
	free(pred);
	return myNode;
}

void unlock(int* myNode) {
	*myNode = false;
	__sync_synchronize();
}

void* thread_work(void* args) {
	for (int i = 0; i < NUM_WORK; i++) {
		int* myNode = lock();
		cnt_global++;
		unlock(myNode);
	}
}

int main(int argc, char const *argv[]) {

	int NUM_THREAD = 16;
	if (argc == 1) {
		NUM_THREAD = 16;
	} else if (argc == 2) {
		NUM_THREAD = atoi(argv[1]);
	} else {
		printf("[error] argc should <= 2 argc: %d\n", argc);
		return -1;
	}
	printf("NUM_THREAD: %d\n", NUM_THREAD);

	pthread_t threads[NUM_THREAD];

	for (int i = 0; i < NUM_THREAD; i++) {
		pthread_create(&threads[i], NULL, thread_work, NULL);
	}
	for (int i = 0; i < NUM_THREAD; i++) {
		pthread_join(threads[i], NULL);
	}
	printf("cnt_global: %d\n", cnt_global);

	free(tail);
	return 0;
}
