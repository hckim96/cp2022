#include <pthread.h>
#include <stdio.h>

#define NUM_THREADS 10
#define NUM_INCREMENT 1000000

thread_local long thread_local_cnt = 0;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void* thread_func(void* arg) {
  long local_cnt = 0;

  for (int i = 0; i < NUM_INCREMENT; i++) {
    thread_local_cnt++;  // Increase thread local count value;
    local_cnt++;         // Increase local count value
  }

  printf("[THREAD] value: %ld\n", thread_local_cnt);

  return (void*)local_cnt;
}

int main(int argc, char* argv[]) {
  pthread_t threads[NUM_THREADS];

  // Create threads
  for (int i = 0; i < NUM_THREADS; i++) {
    if (pthread_create(&threads[i], 0, thread_func, NULL) < 0) {
      printf("error: pthread_create failed!\n");
      return 0;
    }
  }

  // Wait for the threads to end
  long ret;
  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], (void**)&ret);
    // printf("thread %ld: local count -> %ld\n", threads[i], ret);
  }
  printf("[Main] thread local count -> %ld\n", thread_local_cnt);

  return 0;
}
