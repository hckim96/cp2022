#include <iostream>
#include <iomanip>
#include <pthread.h>
#include "Timer.hpp"
#include "WFSnapshot.hpp"
#include "RandomIntGenerator.hpp"

#define UPDATE_TIME 60.0

using namespace std;

RandomIntGenerator randomIntGenerator;
Timer timer;
int NUM_THREADS = -1;
vector<pthread_t> threads;
WFSnapshot<int>* wfsnapshot = nullptr;

void* ThreadFunc(void* arg) {
  long tid = (long)arg;
  long long updateCnt = 0;
  while (true) {
    if (timer.get() >= UPDATE_TIME) break;
    // update thread's register value to random number.
    wfsnapshot->update(tid, randomIntGenerator.generate());
    updateCnt++;
  }
  // return local update count.
  return (void*)updateCnt;
}

int main(int argc, char const *argv[]) {

  // error checking
  if (argc != 2) {
    cout << "[ERROR]: argc is " << argc << " (should be 2)\n";
    cout << "usage: ./run {thread_num}\n";
    return -1;
  }
  if (!(NUM_THREADS = atoi(argv[1]))) {
    cout << "[ERROR]: invalid input, thread_num should be converted to integer.\n";
    cout << "argv[1]: " << argv[1] << '\n';
    return - 1;
  }

  WFSnapshot<int> wfsnapshot_(NUM_THREADS, 0);
  wfsnapshot = &wfsnapshot_;
  threads.resize(NUM_THREADS, 0);

  // timer start
  timer.restart();
  // worker thread creation
  for (long i = 0; i < NUM_THREADS; i++) {
    if (pthread_create(&threads[i], 0, ThreadFunc, (void*)i) < 0) {
      cout << "pthread_create error!\n";
      return -1;
    }
  }

  long long total_update_cnt = 0;
  long long ret;
  // collect result
  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], (void**)&ret);
    cout << "thread_" << i << ": " << ret << '\n';
    total_update_cnt += ret;
  }
  cout << setw(12) << "thread_num" << "\t" << setw(20) << "total_update_count" << '\n';
  cout << setw(12) << NUM_THREADS << "\t" << setw(20) << total_update_cnt << '\n';

  return 0;
}

