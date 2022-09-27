# Project 1: Parallel Join Processing

## Outline of the project

- Get results of queries of join of batches as fast as possible.
- Before query comes in, there's 1s time for preprocessing.
- Next batch comes after getting all the results of current batch. (blocking between batches)

## Optimization

1. Preprocessing
  - cache columns range: (min, max)
  - cache relations tuple number
2. Inter-query parallelism
  - In same batch, queries run asynchronously.
3. Query rewrite
  - add filter at selectInfo(rel, col) which has join predicate that has filter.
  - add filter with it's range.
  - reorder predicate. many filter, smaller relation size node will come first.
4. Filter Scan run skip
  - If col range is all filtered(empty result) skip the iteration of tuples.
5. Intra-Operator parallelism
  - parallel hash join
    - Divide work loop iterating right input.
  - parallel filter scan
    - Divide work loop iterating tuples.
6. Calculate sum while probing.
  - root operator calculate sum while doing it's job.

## Implementation

1. Preprocessing
  - cache columns range: (min, max)
  - cache relations tuple number
```cpp
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
```

save cache globally.


iterate all relations and get range of each columns and size of relation.


2. Inter-query parallelism
  - In same batch, queries run asynchronously.

```cpp

```


3. Query rewrite
  - add filter at selectInfo(rel, col) which has join predicate that has filter.
  - add filter with it's range.
  - reorder predicate. many filter, smaller relation size node will come first.
4. Filter Scan run skip
  - If col range is all filtered(empty result) skip the iteration of tuples.
5. Intra-Operator parallelism
  - parallel hash join
    - Divide work loop iterating right input.
  - parallel filter scan
    - Divide work loop iterating tuples.
6. Calculate sum while probing.
  - root operator calculate sum while doing it's job.


## Third party library

- thread pool
  - https://github.com/bshoshany/thread-pool
