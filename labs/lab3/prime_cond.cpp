/*
requirement:
• Create worker threads at once
• Wake up the threads when job is comes in
• Put the threads to sleep after a job done
• Compare the performance with prime_mt using workload.txt
*/

#include <stdio.h>
#include <pthread.h>
#include <math.h>
#include <queue>

#define NUM_THREAD  10

int thread_ret[NUM_THREAD];

using namespace std;

queue<long> workQ;
pthread_cond_t cond_main = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_worker = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
int doneCnt = 0;
// pthread_mutex_t mutex_main = PTHREAD_MUTEX_INITIALIZER;
// pthread_mutex_t mutex_worker = PTHREAD_MUTEX_INITIALIZER;

int range_start;
int range_end;

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
    // long tid = (long)arg;
    
    while (1) {
        // wakeup
        // try to get tid
        pthread_mutex_lock(&mutex);
        while (!workQ.size()) {
        pthread_cond_wait(&cond_worker, &mutex);
        }
        long tid = workQ.front();
        workQ.pop();
        pthread_mutex_unlock(&mutex);

        // Split range for this thread
        int start = range_start + ((range_end - range_start + 1) / NUM_THREAD) * tid;
        int end = range_start + ((range_end - range_start + 1) / NUM_THREAD) * (tid+1);
        if (tid == NUM_THREAD - 1) {
            end = range_end + 1;
        }
        
        long cnt_prime = 0;
        for (int i = start; i < end; i++) {
            if (IsPrime(i)) {
                cnt_prime++;
            }
        }

        thread_ret[tid] = cnt_prime;
        
        pthread_mutex_lock(&mutex);
        doneCnt++;
        if (doneCnt == NUM_THREAD) pthread_cond_signal(&cond_main);
        pthread_mutex_unlock(&mutex);
    }
    return NULL;
}

int main(void) {
    pthread_t threads[NUM_THREAD];

    for (int i = 0; i < NUM_THREAD; ++i) {
      if (pthread_create(&threads[i], 0, ThreadFunc, NULL) < 0) {
          printf("pthread_create error!\n");
          return 0;
      }
    }
    
    while (1) {
        doneCnt = 0;
        // Input range
        scanf("%d", &range_start);
        if (range_start == -1) {
            break;
        }
        scanf("%d", &range_end);

        // Wake up the threads when job is comes in
        for (long i = 0; i < NUM_THREAD; i++) {
          workQ.push(i);
          pthread_cond_broadcast(&cond_worker);
        }

        pthread_mutex_lock(&mutex);
        while (workQ.size()) {
          pthread_cond_wait(&cond_main, &mutex);
        }
        pthread_mutex_unlock(&mutex);

        // Collect results
        int cnt_prime = 0;
        for (int i = 0; i < NUM_THREAD; i++) {
            cnt_prime += thread_ret[i];
        }
        printf("number of prime: %d\n", cnt_prime);
    }
 
    return 0;
}

