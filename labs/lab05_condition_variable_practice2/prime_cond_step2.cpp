#include <math.h>
#include <pthread.h>
#include <stdio.h>

#define NUM_THREAD 10


int thread_ret[NUM_THREAD];

int range_start;
int range_end;

// For controlling the worker threads
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t main_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

bool is_done = false;
int done_thread_cnt = 0;

bool IsPrime(int n) {
  if (n < 2) {
    return false;
  }

  for (int i = 2; i <= sqrt(n); i++) {
    if (n % i == 0) {
      return false;
    }
  }
  return true;
}

void* ThreadFunc(void* arg) {
  long tid = (long)arg;

  pthread_mutex_lock(&mutex);
  thread_ret[tid] = -1;
  pthread_cond_wait(&cond, &mutex);
  pthread_mutex_unlock(&mutex);

  while (!is_done) {
    // Split range for this thread
    int start =
        range_start + ((range_end - range_start + 1) / NUM_THREAD) * tid;
    int end =
        range_start + ((range_end - range_start + 1) / NUM_THREAD) * (tid + 1);
    if (tid == NUM_THREAD - 1) {
      end = range_end + 1;
    }

    long cnt_prime = 0;
    for (int i = start; i < end; i++) {
      if (IsPrime(i)) {
        cnt_prime++;
      }
    }
    
    // Wait on the conditional variable
    pthread_mutex_lock(&mutex);
    thread_ret[tid] = cnt_prime;
    // wake up the main thread
    if (__sync_fetch_and_add(&done_thread_cnt, 1) == NUM_THREAD - 1) {
      pthread_cond_signal(&main_cond);
    }
    pthread_cond_wait(&cond, &mutex);
    pthread_mutex_unlock(&mutex);
  }

  return NULL;
}

int main(void) {
  pthread_t threads[NUM_THREAD];

  // Create the worker threads and make them fall asleep
  // Maybe the right place to make them sleep is in the ThreadFunc
  for (long i = 0; i < NUM_THREAD; i++) {
    if (pthread_create(&threads[i], 0, ThreadFunc, (void*)i) < 0) {
      printf("pthread_create error!\n");
      return 0;
    }

    while (thread_ret[i] != -1) {
      pthread_yield();
    }
  }

  while (1) {

    done_thread_cnt = 0;

    // Input range
    scanf("%d", &range_start);
    if (range_start == -1) {
      is_done = true;

      // Wake-up the worker threads for termination
      pthread_mutex_lock(&mutex);
      pthread_cond_broadcast(&cond);
      pthread_mutex_unlock(&mutex);
      break;
    }
    scanf("%d", &range_end);

    for (int i = 0; i < NUM_THREAD; i++) {
      thread_ret[i] = -1;
    }

    // Wake the worker threads waiting on the condition variable
    // hint: use broadcast
    pthread_mutex_lock(&mutex);
    pthread_cond_broadcast(&cond);


    // TA: care about lost wake up problem
    pthread_cond_wait(&main_cond, &mutex);
    pthread_mutex_unlock(&mutex);
    // Wait for the worker threads to finish their job
    // while (1) {
    //   bool all_thread_done = true;

    //   for (int i = 0; i < NUM_THREAD; i++) {
    //     if (thread_ret[i] < 0) {
    //       all_thread_done = false;
    //       break;
    //     }
    //   }

    //   if (all_thread_done) {
    //     break;
    //   }
    // }

    // Collect results
    int cnt_prime = 0;
    for (int i = 0; i < NUM_THREAD; i++) {
      cnt_prime += thread_ret[i];
    }
    printf("number of prime: %d\n", cnt_prime);
  }

  // Wait threads end
  for (int i = 0; i < NUM_THREAD; i++) {
    pthread_join(threads[i], NULL);
  }

  return 0;
}
