#include <iostream>
#include "Joiner.hpp"
#include "Parser.hpp"
// #include <pthread.h>
#include "ThreadPool.h"
using namespace std;

// query in  one batch max: 30 ~ 40
uint64_t mNumThread = 100;
ThreadPool mpool(mNumThread);

uint64_t fsNumThread = 100;
ThreadPool fspool(fsNumThread);

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
   vector<Joiner> joiners(mNumThread);
   for (int i = 0; i < mNumThread; ++i) {
      joiners[i] = joiner;
   }

   QueryInfo i;
   int idx = 0;
   vector<future<string> > results;
   vector<string> lines;
   while (getline(cin, line)) {
      if (line == "F") {
         for (auto&& e: results) {
            cout << e.get();
         }
         results.clear();
         lines.clear();
         idx = 0;
         continue;
      } // End of a batch
      lines.push_back(line);
      string tmp = line;
      results.emplace_back(
         mpool.enqueue([&joiners, &lines, idx] {
            QueryInfo i;
            i.parseQuery(lines[idx]);
            return joiners[idx].join(i);
         })
      );
      idx++;
   }
   return 0;
}