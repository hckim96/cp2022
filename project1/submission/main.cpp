#include <iostream>
#include "Joiner.hpp"
#include "Parser.hpp"
#include "BS_thread_pool.hpp"
using namespace std;

// query in  one batch max: 30 ~ 40
uint64_t fsNumThread = 40;
BS::thread_pool pool(fsNumThread);
#ifdef MY_DEBUG
std::mutex cerrMutex;
#endif


// rel, colId -> range(pair)
vector<vector<pair<uint64_t, uint64_t> > > rangeCache;
vector<uint64_t> relationSizeCache;

// rel col key = cnt
vector<vector<unordered_map<uint64_t, uint64_t> > > histogram;

void cacheRelationRange(Joiner joiner) {
   rangeCache.resize(joiner.relations.size());
   histogram.resize(joiner.relations.size());
   for (int i = 0; i < joiner.relations.size(); ++i) {
      rangeCache[i].resize(joiner.relations[i].columns.size());
      histogram[i].resize(joiner.relations[i].columns.size());
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
            histogram[i][j][r.columns[j][rid]]++;
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
   // Preparation phase (not timed)
   // Build histograms, indexes,...
   vector<Joiner> joiners(fsNumThread);
   for (int i = 0; i < fsNumThread; ++i) {
      joiners[i] = joiner;
   }
   #if defined(MY_DEBUG)
   // Timer preprocess;
   #endif // MY_DEBUG
   
   cacheRelationRange(joiner);
   #if defined(MY_DEBUG)
   Timer preprocess;
   preprocess.cerrget("preprocess: ");
   #endif // MY_DEBUG

   int idx = 0;
   vector<future<string> > results;
   vector<string> lines(fsNumThread);
   while (getline(cin, line)) {
      if (line == "F") {
         for (auto&& e: results) {
            cout << e.get();
         }
         results.clear();
         idx = 0;
         continue;
      } // End of a batch
      lines[idx] = line;
      string tmp = line;
      results.emplace_back(
         pool.submit([&joiners, &lines, idx] {
            QueryInfo i;
            #ifdef MY_DEBUG
            Timer p;
            #endif
            i.parseQuery(lines[idx]);
            #ifdef MY_DEBUG
            // auto parseTime = p.get();
            // Timer t;
            #endif
            
            auto ret = joiners[idx].join(i);
            #ifdef MY_DEBUG
            // auto joinTime = t.get();
            // cerrMutex.lock();
            // cerr << "Thread " << idx << " parseTime :" <<  (double)(parseTime * 1000) << '\n';
            // cerr << "Thread " << idx << " joinTime :" <<  (double)(joinTime * 1000) << '\n';
            // cerrMutex.unlock();
            #endif
            return ret;
         })
      );
      idx++;
   }
   return 0;
}