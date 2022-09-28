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


main.cpp
```cpp
   while (getline(cin, line)) {
      if (line == "F") {
         // wait all query in current batch is end.
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
         // submit task to the thread pool
         pool.submit([&joiners, &lines, idx] {
            QueryInfo i;
            i.parseQuery(lines[idx]);
            return joiners[idx].join(i);
         })
      );
      idx++;
   }
```


- Used third party library which implemented thread pool.
- When query comes in, push task to the queue.
- When batch end, wait for task is done.

3. Query rewrite
    - add filter at selectInfo(rel, col) which has join predicate that has filter.

parser.cpp
```cpp
void QueryInfo::sameSelect() {
  for (auto& pInfo: predicates) {
    bool found = false;
    for (auto& s: same) {
      if (s.count(pInfo.left)) {
        found = true;
        s.insert(pInfo.right);
        break;
      } else if (s.count(pInfo.right)) {
        found = true;
        s.insert(pInfo.left);
        break;
      }
    }
    if (!found) {
      same.push_back({pInfo.left, pInfo.right});
    }
  }
  for (int i = 0; i < selections.size(); ++i) {
    for (int j = 0; j < i; ++j) {
      bool isSameSum = false;
      for (auto s: same) {
        if (s.count(selections[i]) && s.count(selections[j])) {
          isSameSum = true;
          break;
        }
      }
      if (isSameSum) {
        selections[i] = selections[j];
        break;
      }
    }
  }
}

```
first make vector of set of same cols by seeing predicates.(std::vector<std::set<SelectInfo> > same)


ex) 1.2 = 3.0 & 3.0 = 0.1 & 1.1 = 0.2  -> {{1.2, 3.0, 0.1} {1.1, 0.2}}

```cpp
void QueryInfo::addMoreFilterWithPredicates() {

  int prevSize = filters.size();
  for (int i = 0; i < prevSize; ++i) {
    auto fInfo = filters[i];
    for (auto s: same) {
      auto it = s.find(fInfo.filterColumn);
      if (it != s.end()) {
        for (auto col: s) {
          if (col == *it) continue;
          filters.emplace_back(col, fInfo.constant, fInfo.comparison);
        }
        break;
      }
    }
  }
}

```


by seeing same set made at sameSelect.


add filters of cols to the other cols in the same set


- add filter with it's range.

```cpp
void QueryInfo::addFilterWithPredicateAndColRange() {
  auto getIntersection = [](pair<uint64_t, uint64_t>& p1, pair<uint64_t, uint64_t>& p2) {
    return make_pair(std::max(p1.first, p2.first), std::min(p1.second, p2.second));
  };

  for (auto& s: same) {
    pair<uint64_t, uint64_t> intersection = {0, UINT64_MAX};
    for (auto& sInfo: s) {
      intersection = getIntersection(intersection, rangeCache[sInfo.relId][sInfo.colId]);
    }
    // if (intersection.first > intersection.second) continue;
    for (auto& sInfo: s) {
      if (intersection.first != 0) filters.emplace_back(sInfo, intersection.first - 1, FilterInfo::Greater);
      if (intersection.second != UINT64_MAX) filters.emplace_back(sInfo, intersection.second + 1, FilterInfo::Less);
    }
  }

  for (auto& pInfo: predicates) {
    auto left = pInfo.left;
    auto right = pInfo.right;

    auto lRange = rangeCache[left.relId][left.colId];
    auto rRange = rangeCache[right.relId][right.colId];
    auto [l, r] = getIntersection(lRange, rRange);

    if (l > r) {
      // no intersection
      filters.emplace_back(left, lRange.second, FilterInfo::Greater);
      filters.emplace_back(right, rRange.second, FilterInfo::Greater);
    } else if (l == r) {
      filters.emplace_back(left, l, FilterInfo::Equal);
      filters.emplace_back(right, l, FilterInfo::Equal);
    } else {
      if (lRange.first != l && l) filters.emplace_back(left, l - 1, FilterInfo::Greater);
      if (lRange.second != r && r) filters.emplace_back(left, r + 1, FilterInfo::Less);
      if (rRange.first != l && l) filters.emplace_back(right, l - 1, FilterInfo::Greater);
      if (rRange.second != r && r) filters.emplace_back(right, r + 1, FilterInfo::Less);
    }
  }
}
```


given query 0.1 = 1.0 and 0.1 < 3000 


where range(1.0) is [2000, 10000] and range(0.1) is [0, 5000]


then filter 0.1 > 2000 and 1.0 < 5000 can be added

- reorder predicate. many filter, smaller relation size node will come first.

```cpp
void QueryInfo::sortPredicates() {
  auto f = [&filters = filters](PredicateInfo& p1, PredicateInfo& p2) {
    int fs1 = 0;
    int fs2 = 0;
    int fs3 = 0;
    int fs4 = 0;
    uint64_t relsize1 = relationSizeCache[p1.left.relId];
    uint64_t relsize2 = relationSizeCache[p1.right.relId];
    uint64_t relsize3 = relationSizeCache[p2.left.relId];
    uint64_t relsize4 = relationSizeCache[p2.right.relId];
    for (auto& f: filters) {
      if (f.filterColumn == p1.left) fs1++;
      if (f.filterColumn == p1.right) fs2++;
      if (f.filterColumn == p2.left) fs3++;
      if (f.filterColumn == p2.right) fs3++;
    }
    
    if (fs1 < fs2) swap(p1.left, p1.right);
    if (fs1 == fs2 && relsize1 > relsize2) swap(p1.left, p1.right);
    if (fs3 < fs4) swap(p2.left, p2.right);
    if (fs3 == fs4 && relsize3 > relsize4) swap(p2.left, p2.right);

    if (fs1 + fs2 > fs3 + fs4) return true;
    if (fs1 + fs2 == fs3 + fs4 && relsize1 + relsize2 < relsize3 + relsize4) return true;
    return false;
  };

  sort(predicates.begin(), predicates.end(), f);
}
```

sort predicates by order (applied filter number (ascending), relations size (descending))


many filter, small relation size will come first


After that, finalizing query.
```cpp
void QueryInfo::finalize() {
  auto getIntersection = [](pair<uint64_t, uint64_t>& p1, pair<uint64_t, uint64_t>& p2) {
    return make_pair(std::max(p1.first, p2.first), std::min(p1.second, p2.second));
  };
  auto applyFilterToRange = [](pair<uint64_t, uint64_t>& p1, FilterInfo& filter) {
    auto getIntersection = [](pair<uint64_t, uint64_t>& p1, pair<uint64_t, uint64_t>& p2) {
      return make_pair(std::max(p1.first, p2.first), std::min(p1.second, p2.second));
    };
    pair<uint64_t, uint64_t> tmp;
    switch (filter.comparison) {
      case FilterInfo::Comparison::Equal:
        tmp = {filter.constant, filter.constant};
        break;
      case FilterInfo::Comparison::Greater:
        if (filter.constant == UINT64_MAX) return make_pair(1UL, 0UL); // empty
        tmp = {filter.constant + 1, UINT64_MAX};
        break;
      case FilterInfo::Comparison::Less:
        if (filter.constant == 0) return make_pair(1UL, 0UL); // empty
        tmp = {0, filter.constant - 1};
        break;
    };
    return getIntersection(p1, tmp);
  };

  vector<FilterInfo> newFilters;
  map<SelectInfo, pair<uint64_t, uint64_t> > m;
  for (auto& fInfo: filters) {
    if (m.count(fInfo.filterColumn)) {
      m[fInfo.filterColumn] = applyFilterToRange(m[fInfo.filterColumn], fInfo);
    } else {
      pair<uint64_t, uint64_t> p = {0UL, UINT64_MAX};
      m[fInfo.filterColumn] = applyFilterToRange(p, fInfo);
    }
  }
  for (auto& [a, b]: m) {
    auto range = rangeCache[a.relId][a.colId];
    b = getIntersection(b, range);
    if (b.first > b.second) {
      newFilters.emplace_back(a, UINT64_MAX, FilterInfo::Greater);
      continue;
    }
    if (range.first != b.first) newFilters.emplace_back(a, b.first - 1, FilterInfo::Greater);
    if (range.second != b.second) newFilters.emplace_back(a, b.second + 1, FilterInfo::Less);
  }
  filters = newFilters;
}

```


merge filters at one col if possible. ex) 1.2 < 1000 & 1.2 < 10000 -> 1.2 < 1000


remove redundant or all tuples passing filters.

4. Filter Scan run skip
    - If col range is all filtered(empty result) skip the iteration of tuples.

```cpp
void FilterScan::run()
  // Run
{

  // check any col is not in filter range
  bool emptyResult = false;
  for (auto& f: filters) {
    auto colRange = rangeCache[f.filterColumn.relId][f.filterColumn.colId];
    switch (f.comparison) {
      case FilterInfo::Comparison::Equal:
        if (f.constant < colRange.first || f.constant > colRange.second) emptyResult = true;
        break;
      case FilterInfo::Comparison::Greater:
        if (f.constant >= colRange.second) emptyResult = true;
        break;
      case FilterInfo::Comparison::Less:
        if (f.constant <= colRange.first) emptyResult = true;
        break;
    };
    if (emptyResult) {
      return;
    }
  }
  // check filter intersection is empty

  /*
    helper function to get range of val after applied filter f to the range p1
  */
  auto applyFilterToRange = [](pair<uint64_t, uint64_t>& p1, FilterInfo& filter) {

    /*
      get intersection of two closed range
      if left > right, no intersection.
    */
    auto getIntersection = [](pair<uint64_t, uint64_t>& p1, pair<uint64_t, uint64_t>& p2) {
      return make_pair(std::max(p1.first, p2.first), std::min(p1.second, p2.second));
    };
    pair<uint64_t, uint64_t> tmp;
    switch (filter.comparison) {
      case FilterInfo::Comparison::Equal:
        tmp = {filter.constant, filter.constant};
        break;
      case FilterInfo::Comparison::Greater:
        if (filter.constant == UINT64_MAX) return make_pair(1UL, 0UL); // empty
        tmp = {filter.constant + 1, UINT64_MAX};
        break;
      case FilterInfo::Comparison::Less:
        if (filter.constant == 0) return make_pair(1UL, 0UL); // empty
        tmp = {0, filter.constant - 1};
        break;
    };
    return getIntersection(p1, tmp);
  };
  for (int i = 0; i < filters.size(); ++i) {
    pair<uint64_t, uint64_t> intersection = {0, UINT64_MAX};
    intersection = applyFilterToRange(intersection, filters[i]);
    if (intersection.first > intersection.second) {
      return;
    }
    for (int j = 0; j < filters.size(); ++j) {
      if (i == j) continue;
      if (filters[i].filterColumn == filters[j].filterColumn) {
        // rel, col same
        intersection = applyFilterToRange(intersection, filters[j]);
        if (intersection.first > intersection.second) {
          return;
        }
      }
    }
  }
  //...
```

check any col's range has no intersection with filter.


check filter intersection is empty.


if any of those is true, then no need to iterate tuples. (empty result)


5. Intra-Operator parallelism
    - parallel hash join
        - Divide work loop iterating right input.

```cpp
void ParallelHashJoin::run()
  // Run
{
  //...

  // Build phase
  auto leftKeyColumn=leftInputData[leftColId];
  hashTable.reserve(left->resultSize*2);
  for (uint64_t i=0,limit=i+left->resultSize;i!=limit;++i) {
    hashTable.emplace(leftKeyColumn[i],i);
  }

  // Probe phase

  // probing multi threading.
  // divide full scan of right input.
  auto rightKeyColumn=rightInputData[rightColId];
  workerCnt = getProperWorkerCnt(right->resultSize, MIN_WORK_SIZE, JOIN_THREAD_NUM);
  auto workSize = right->resultSize / workerCnt;
  vector<future<void> > futures;
  for (int tid = 0; tid < workerCnt; ++tid) {
    auto s = workSize * tid;
    auto e = workSize * (tid + 1);
    if (tid == workerCnt - 1) e = right->resultSize;
    futures.emplace_back( joinpool.submit([this, &rightKeyColumn, tid, s, e] {
      vector<uint64_t> localSums(copyLeftData.size() + copyRightData.size(), 0);
      uint64_t localResultSize = 0;
      for (auto i = s; i != e; ++i) {
        auto rightKey=rightKeyColumn[i];
        auto range=hashTable.equal_range(rightKey);
        for (auto iter=range.first;iter!=range.second;++iter) {
          auto leftId = iter->second;
          auto rightId = i;

          unsigned relColId=0;
          for (unsigned cId=0;cId<copyLeftData.size();++cId) {
            if (isRoot) {
              localSums[relColId] += copyLeftData[cId][leftId];
            }
            paralleltmpResults[tid][relColId++].push_back(copyLeftData[cId][leftId]);
          }

          for (unsigned cId=0;cId<copyRightData.size();++cId) {
            if (isRoot) {
              localSums[relColId] += copyRightData[cId][rightId];
            }
            paralleltmpResults[tid][relColId++].push_back(copyRightData[cId][rightId]);
          }
          localResultSize += 1;
        }
      }
      if (isRoot) {
        // calculate sum while probing
        for (int i = 0; i < localSums.size(); ++i) {
        __sync_fetch_and_add(&tmpSums[i], localSums[i]);
        }
      }
      __sync_fetch_and_add(&resultSize, localResultSize);
    })
    );
  }

  // wait all threads end.
  for (auto&& f: futures) {
    f.wait();
  }
}
```

getProperWorkerCnt() is helper function that


get proper worker cnt.


that 1 <= && <= maxThreadNum


and each worker can work with equal or more than minWorkSize


hashjoin::run() is same as original until building hashTable.


divide rows to workers to reduce iterating all rows of right column.

- parallel filter scan
    - Divide work loop iterating tuples.

```cpp
void FilterScan::run()
  // Run
{
//...
  workerCnt = getProperWorkerCnt(relation.size, FS_MIN_WORK_SIZE, FSSCAN_THREAD_NUM);
  auto workSize = relation.size / workerCnt;
  vector<future<void> > futures;
  for (int tid = 0; tid < workerCnt; ++tid) {
    auto s = workSize * tid;
    auto e = workSize * (tid + 1);
    if (tid == workerCnt - 1) e = relation.size;
    futures.emplace_back(fsscanpool.submit([this, tid, s, e]{
      uint64_t localResultSize = 0;
      for (uint64_t i = s; i != e; ++i) {
        bool pass=true;
        for (auto& f : filters) {
          pass&=applyFilter(i,f);
        }
        if (pass) {
          for (unsigned cId=0;cId<inputData.size();++cId) {
            paralleltmpResults[tid][cId].push_back(inputData[cId][i]);
          }
          localResultSize++;
        }
      }
      __sync_fetch_and_add(&resultSize, localResultSize);
    }));
  }

  for (auto&& e: futures) {
    e.wait();
  }
}


```

divide iterating to workers input rows and applying filters.


6. Calculate sum while probing.
    - root operator calculate sum while doing it's job.

in SelfJoin
```cpp
/*
  calculate sum if it's root.
*/
void SelfJoin::copy2Result(uint64_t id)
  // Copy to result
{
  for (unsigned cId=0;cId<copyData.size();++cId) {
    if (isRoot) {
      tmpSums[cId] += copyData[cId][id];
    }
    tmpResults[cId].push_back(copyData[cId][id]);
  }
  ++resultSize;
}
```

and in ParallelHashJoin


```cpp
void ParallelHashJoin::run()
  // Run
{
// ...

  auto workSize = right->resultSize / workerCnt;
  vector<future<void> > futures;
  for (int tid = 0; tid < workerCnt; ++tid) {
    auto s = workSize * tid;
    auto e = workSize * (tid + 1);
    if (tid == workerCnt - 1) e = right->resultSize;
    futures.emplace_back( joinpool.submit([this, &rightKeyColumn, tid, s, e] {
      vector<uint64_t> localSums(copyLeftData.size() + copyRightData.size(), 0);
      uint64_t localResultSize = 0;
      for (auto i = s; i != e; ++i) {
        auto rightKey=rightKeyColumn[i];
        auto range=hashTable.equal_range(rightKey);
        for (auto iter=range.first;iter!=range.second;++iter) {
          auto leftId = iter->second;
          auto rightId = i;

          unsigned relColId=0;
          for (unsigned cId=0;cId<copyLeftData.size();++cId) {
            if (isRoot) {
              localSums[relColId] += copyLeftData[cId][leftId];
            }
            paralleltmpResults[tid][relColId++].push_back(copyLeftData[cId][leftId]);
          }

          for (unsigned cId=0;cId<copyRightData.size();++cId) {
            if (isRoot) {
              localSums[relColId] += copyRightData[cId][rightId];
            }
            paralleltmpResults[tid][relColId++].push_back(copyRightData[cId][rightId]);
          }
          localResultSize += 1;
        }
      }
      if (isRoot) {
        // calculate sum while probing
        for (int i = 0; i < localSums.size(); ++i) {
        __sync_fetch_and_add(&tmpSums[i], localSums[i]);
        }
      }
      __sync_fetch_and_add(&resultSize, localResultSize);
    })
    );
  }

  // wait all threads end.
  for (auto&& f: futures) {
    f.wait();
  }
}

```

if root, calculate sum while copying the result.


in parallel hash join, each thread is adding to shared data.


So used made it atomic with __sync_fetch_and_add function

## Third party library

- thread pool
  - https://github.com/bshoshany/thread-pool
