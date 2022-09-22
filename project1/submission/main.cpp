#include <iostream>
#include "Joiner.hpp"
#include "Parser.hpp"
#include "ThreadPool.h"
using namespace std;

// query in  one batch max: 30 ~ 40
uint64_t fsNumThread = 100;
ThreadPool pool(fsNumThread);


// unordered_map<SelectInfo, pair<uint64_t, uint64_t> > 
// rel, colId -> range(pair)
vector<vector<pair<uint64_t, uint64_t> > > rangeCache;
void cacheRelationRange(Joiner joiner) {
   rangeCache.resize(joiner.relations.size());
   for (int i = 0; i < joiner.relations.size(); ++i) {
      rangeCache[i].resize(joiner.relations[i].columns.size());
   }
   // care
   for (uint64_t i = 0; i < joiner.relations.size(); ++i) {
      auto& r = joiner.relations[i];
      uint64_t min_, max_;
      min_ = 18446744073709551615; // 2^64 - 1
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
   // Preparation phase (not timed)
   // Build histograms, indexes,...
   vector<Joiner> joiners(fsNumThread);
   for (int i = 0; i < fsNumThread; ++i) {
      joiners[i] = joiner;
   }
   cacheRelationRange(joiner);

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
         pool.enqueue([&joiners, &lines, idx] {
            QueryInfo i;
            i.parseQuery(lines[idx]);
            return joiners[idx].join(i);
         })
      );
      idx++;
   }
   return 0;
}