#include <iostream>
#include "Joiner.hpp"
#include "Parser.hpp"
#include "BS_thread_pool.hpp"
#include "debug.hpp"
using namespace std;

// query in  one batch max: 30 ~ 40
BS::thread_pool pool(THREAD_NUM);
BS::thread_pool joinpool(JOIN_THREAD_NUM);
BS::thread_pool fsscanpool(FSSCAN_THREAD_NUM);

#ifdef MY_DEBUG
std::mutex cerrMutex;
#endif


// rel, colId -> range(min, max)
vector<vector<pair<uint64_t, uint64_t> > > rangeCache;
// rel -> relation tuple num
vector<uint64_t> relationSizeCache;

void cacheRelationRange(Joiner joiner) {
   rangeCache.resize(joiner.relations.size());
   for (int i = 0; i < joiner.relations.size(); ++i) {
      rangeCache[i].resize(joiner.relations[i].columns.size());
   }
   // care
   for (uint64_t i = 0; i < joiner.relations.size(); ++i) {
      auto& r = joiner.relations[i];
      relationSizeCache.push_back(r.size);
      uint64_t min_, max_;
      min_ = UINT64_MAX; // 2^64 - 1
      max_ = 0;
      for (uint64_t j = 0; j < r.columns.size(); ++j) {
         for (uint64_t rid = 0; rid < r.size; rid++) {
            min_ = min(min_, r.columns[j][rid]);
            max_ = max(max_, r.columns[j][rid]);
         }
         rangeCache[i][j] = {min_, max_};
      }
   }
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
   #if defined(MY_DEBUG)
   Timer preprocess;
   #endif // MY_DEBUG
   // Preparation phase (not timed)
   // Build histograms, indexes,...

   // copy joiners that each thread will use
   vector<Joiner> joiners(THREAD_NUM);
   for (int i = 0; i < THREAD_NUM; ++i) {
      joiners[i] = joiner;
   }

   // preprocess
   // cache all col's range : (min, max) -> used in filter optimizing
   // cache each relation's size -> used in predicate reordering
   cacheRelationRange(joiner);
   #if defined(MY_DEBUG)
   preprocessTime += preprocess.get();
   #endif // MY_DEBUG

   int idx = 0;
   #if defined(MY_DEBUG)
   int totalIdx = 0;
   #endif // MY_DEBUG
   
   vector<future<string> > results;
   vector<string> lines(THREAD_NUM);
   while (getline(cin, line)) {
      #if defined(MY_DEBUG)
      Timer batch;
      #endif // MY_DEBUG
      if (line == "F") {
         // wait all query in current batch is end.
         for (auto&& e: results) {
            cout << e.get();
         }
         results.clear();
         idx = 0;
         #if defined(MY_DEBUG)
         batch.cerrget("batch: ");
         batch.restart();
         #endif // MY_DEBUG
         continue;
      } // End of a batch
      lines[idx] = line;
      string tmp = line;
      results.emplace_back(
         // submit task to the thread pool
         pool.submit([&joiners, &lines, idx] {
            QueryInfo i;
            i.parseQuery(lines[idx]);
            return joiners[idx].join(i);
         })
      );
      idx++;
      #if defined(MY_DEBUG)
      totalIdx++;
      #endif // MY_DEBUG
   }

   #if defined(MY_DEBUG)
   cerrMutex.lock();
   cerr << "preprocessTime: " << preprocessTime << '\n';
   cerr << "indexBuildTime: " << indexBuildTime << '\n';
   cerr << "JoinHashBuildTime: " << JoinHashBuildTime << '\n';
   cerr << "JoinProbingTime: " << JoinProbingTime << '\n';
   cerr << "INLJoinTime: " << INLJoinTime << '\n';
   cerr << "SelfJoinTime: " << SelfJoinTime << '\n';
   cerr << "SMJoinTime: " << SMJoinTime << '\n';
   cerr << "FSScanTime: " << FSScanTime << '\n';
   cerrMutex.unlock();
   
   #endif // MY_DEBUG
   
   return 0;
}