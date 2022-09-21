#include <iostream>
#include "Joiner.hpp"
#include "Parser.hpp"
#include <pthread.h>
#include "ThreadPool.h"
using namespace std;

// query in  one batch max: 30 ~ 40
#define NUM_THREADS 40

vector<Joiner> joiners(NUM_THREADS);
vector<QueryInfo> queryInfos(NUM_THREADS);
pthread_t threads[NUM_THREADS];
vector<string> results(NUM_THREADS);

uint64_t fsNumThread = 40;
ThreadPool pool(fsNumThread);

void* thread_func(void* arg) {
   int idx = *(int*)arg;
   results[idx] = joiners[idx].join(queryInfos[idx]);
   return (void*)0;
}

//---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
   
   Joiner joiner;
   // Read join relations
   string line;
   while (getline(cin, line)) {
      if (line == "Done") break;
      joiner.addRelation(line.c_str());
   }
   // Preparation phase (not timed)
   // Build histograms, indexes,...
   for (int i = 0; i < NUM_THREADS; ++i) {
      joiners[i] = joiner;
   }

   QueryInfo i;
   int idx = 0;
   while (getline(cin, line)) {
      int* arg = (int*)malloc(sizeof(int));
      if (line == "F") {
         for (int j = 0; j < idx; ++j) {
            pthread_join(threads[j], NULL);
            cout << results[j];
         }
         idx = 0;
         continue;
      } // End of a batch
      i.parseQuery(line);
      queryInfos[idx] = i;
      *arg = idx;
      if (pthread_create(&threads[idx], NULL, thread_func, (void*)arg) < 0) {
         cout << "pthread_create error";
         return -1;
      }
      idx++;
   }
   return 0;
}
