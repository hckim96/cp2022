#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define NUM_THREAD		16
#define NUM_WORK		1000000

int cnt_global;
/* to allocate cnt_global & object_tas in different cache lines */
int gap[128];

struct clhlock {
	int flag;
};

clhlock* tail;

clhlock* lock(void) {
	clhlock* lock_node = (clhlock*)malloc(sizeof(clhlock));
	lock_node->flag = 0;

	clhlock* prev_node = __sync_lock_test_and_set(&tail, lock_node);
	while (prev_node != NULL && prev_node->flag == 0) {
		//pthread_yield();
	}
	free(prev_node); // :: ,,safe to free here,,

	return lock_node;
}

void unlock(clhlock* lock_node) {
	lock_node->flag = 1;
	__sync_synchronize();
}

void* work(void* args) {
	for (int i = 0; i < NUM_WORK; i++) {
		clhlock* lock_node = lock();
		cnt_global++;
		unlock(lock_node);
	}
}

int main(void) {
	pthread_t threads[NUM_THREAD];

	for (int i = 0; i < NUM_THREAD; i++) {
		pthread_create(&threads[i], NULL, work, NULL);
	}
	for (int i = 0; i < NUM_THREAD; i++) {
		pthread_join(threads[i], NULL);
	}
	printf("cnt_global: %d\n", cnt_global);

	return 0;
}
