#include <stdio.h>
#include <pthread.h>

#define NUM_THREAD		16
#define NUM_WORK		1000000

int cnt_global;
/* to allocate cnt_global & object_tas in different cache lines */
int gap[128];
int object_tas;

void lock(int* lock_object) {
	while (__sync_lock_test_and_set(lock_object, 1) == 1)  {}
}

void unlock(int* lock_object) {
	*lock_object = 0;
	__sync_synchronize();
}

void* thread_work(void* args) {
	for (int i = 0; i < NUM_WORK; i++) {
		lock(&object_tas);
		cnt_global++;
		unlock(&object_tas);
	}
}

int main(void) {
	pthread_t threads[NUM_THREAD];

	for (int i = 0; i < NUM_THREAD; i++) {
		pthread_create(&threads[i], NULL, thread_work, NULL);
	}
	for (int i = 0; i < NUM_THREAD; i++) {
		pthread_join(threads[i], NULL);
	}
	printf("cnt_global: %d\n", cnt_global);

	return 0;
}
