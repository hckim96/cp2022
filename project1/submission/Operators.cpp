#include <Operators.hpp>
#include <cassert>
#include <iostream>
#include <map>
//---------------------------------------------------------------------------
using namespace std;
extern BS::thread_pool joinpool;
//---------------------------------------------------------------------------
bool Scan::require(SelectInfo info)
  // Require a column and add it to results
{
  if (info.binding!=relationBinding)
    return false;
  assert(info.colId<relation.columns.size());
  resultColumns.push_back(relation.columns[info.colId]);
  select2ResultColId[info]=resultColumns.size()-1;
  return true;
}
//---------------------------------------------------------------------------
void Scan::run()
  // Run
{
  // Nothing to do
  resultSize=relation.size;
}
//---------------------------------------------------------------------------
vector<uint64_t*> Scan::getResults()
  // Get materialized results
{
  return resultColumns;
}
//---------------------------------------------------------------------------
bool FilterScan::require(SelectInfo info)
  // Require a column and add it to results
{
  if (info.binding!=relationBinding)
    return false;
  assert(info.colId<relation.columns.size());
  if (select2ResultColId.find(info)==select2ResultColId.end()) {
    // Add to results
    inputData.push_back(relation.columns[info.colId]);
    tmpResults.emplace_back();
    if (isRoot) {
      tmpSums.emplace_back(0UL);
    }
    unsigned colId=tmpResults.size()-1;
    select2ResultColId[info]=colId;
  }
  return true;
}
//---------------------------------------------------------------------------
void FilterScan::copy2Result(uint64_t id)
  // Copy to result
{
  for (unsigned cId=0;cId<inputData.size();++cId) {
    if (isRoot) {
      tmpSums[cId] += inputData[cId][id];
    }
    tmpResults[cId].push_back(inputData[cId][id]);
  }
  ++resultSize;
}
//---------------------------------------------------------------------------
bool FilterScan::applyFilter(uint64_t i,FilterInfo& f)
  // Apply filter
{
  auto compareCol=relation.columns[f.filterColumn.colId];
  auto constant=f.constant;
  switch (f.comparison) {
    case FilterInfo::Comparison::Equal:
      return compareCol[i]==constant;
    case FilterInfo::Comparison::Greater:
      return compareCol[i]>constant;
    case FilterInfo::Comparison::Less:
       return compareCol[i]<constant;
  };
  return false;
}
//---------------------------------------------------------------------------
extern vector<vector<pair<uint64_t, uint64_t> > > rangeCache;

void FilterScan::run()
  // Run
{
  #ifdef MY_DEBUG
  Timer fs;
  #endif

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
      #if defined(MY_DEBUG)
      FSScanTime += fs.get();
      #endif // MY_DEBUG
      return;
    }
  }
  // check filter intersection is empty
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
  for (int i = 0; i < filters.size(); ++i) {
    pair<uint64_t, uint64_t> intersection = {0, UINT64_MAX};
    intersection = applyFilterToRange(intersection, filters[i]);
    if (intersection.first > intersection.second) {
      #if defined(MY_DEBUG)
      FSScanTime += fs.get();
      #endif // MY_DEBUG
      
      return;
    }
    for (int j = 0; j < filters.size(); ++j) {
      if (i == j) continue;
      if (filters[i].filterColumn == filters[j].filterColumn) {
        // rel, col same
        intersection = applyFilterToRange(intersection, filters[j]);
        if (intersection.first > intersection.second) {
          #if defined(MY_DEBUG)
          FSScanTime += fs.get();
          #endif // MY_DEBUG
          return;
        }
      }
    }
  }

  for (uint64_t i=0;i<relation.size;++i) {
    bool pass=true;
    for (auto& f : filters) {
      pass&=applyFilter(i,f);
    }
    if (pass)
      copy2Result(i);
  }
  #ifdef MY_DEBUG
  FSScanTime += fs.get();
  #endif
}
//---------------------------------------------------------------------------
vector<uint64_t> Operator::getSums() {
  return tmpSums;
}
vector<uint64_t*> Operator::getResults()
  // Get materialized results
{
  vector<uint64_t*> resultVector;
  for (auto& c : tmpResults) {
    resultVector.push_back(c.data());
  }
  return resultVector;
}
//---------------------------------------------------------------------------
bool Join::require(SelectInfo info)
  // Require a column and add it to results
{
  if (requestedColumns.count(info)==0) {
    bool success=false;
    if(left->require(info)) {
      requestedColumnsLeft.emplace_back(info);
      success=true;
    } else if (right->require(info)) {
      success=true;
      requestedColumnsRight.emplace_back(info);
    }
    if (!success)
      return false;

    tmpResults.emplace_back();
    if (isRoot) {
      tmpSums.emplace_back(0UL);
    }
    requestedColumns.emplace(info);
  }
  return true;
}
//---------------------------------------------------------------------------
void Join::copy2Result(uint64_t leftId,uint64_t rightId)
  // Copy to result
{
  unsigned relColId=0;
  for (unsigned cId=0;cId<copyLeftData.size();++cId) {
    if (isRoot) {
      tmpSums[relColId] += copyLeftData[cId][leftId];
    }
    tmpResults[relColId++].push_back(copyLeftData[cId][leftId]);
  }

  for (unsigned cId=0;cId<copyRightData.size();++cId) {
    if (isRoot) {
      tmpSums[relColId] += copyRightData[cId][rightId];
    }
    tmpResults[relColId++].push_back(copyRightData[cId][rightId]);
  }
  ++resultSize;
}
//---------------------------------------------------------------------------
void Join::run()
  // Run
{
  left->require(pInfo.left);
  right->require(pInfo.right);
  left->run();
  right->run();
  #ifdef MY_DEBUG
  Timer jj;
  #endif


  // Use smaller input for build
  if (left->resultSize>right->resultSize) {
    swap(left,right);
    swap(pInfo.left,pInfo.right);
    swap(requestedColumnsLeft,requestedColumnsRight);
  }

  auto leftInputData=left->getResults();
  auto rightInputData=right->getResults();

  // Resolve the input columns
  unsigned resColId=0;
  for (auto& info : requestedColumnsLeft) {
    copyLeftData.push_back(leftInputData[left->resolve(info)]);
    select2ResultColId[info]=resColId++;
  }
  for (auto& info : requestedColumnsRight) {
    copyRightData.push_back(rightInputData[right->resolve(info)]);
    select2ResultColId[info]=resColId++;
  }

  auto leftColId=left->resolve(pInfo.left);
  auto rightColId=right->resolve(pInfo.right);

  #if defined(MY_DEBUG)
  Timer buildAndProbe;
  #endif // MY_DEBUG
  
  // Build phase
  auto leftKeyColumn=leftInputData[leftColId];
  hashTable.reserve(left->resultSize*2);
  for (uint64_t i=0,limit=i+left->resultSize;i!=limit;++i) {
    hashTable.emplace(leftKeyColumn[i],i);
  }

  #if defined(MY_DEBUG)
  JoinHashBuildTime += buildAndProbe.get();
  buildAndProbe.restart();
  #endif // MY_DEBUG
  // Probe phase
  auto rightKeyColumn=rightInputData[rightColId];
  for (uint64_t i=0,limit=i+right->resultSize;i!=limit;++i) {
    auto rightKey=rightKeyColumn[i];
    auto range=hashTable.equal_range(rightKey);
    for (auto iter=range.first;iter!=range.second;++iter) {
      copy2Result(iter->second,i);
    }
  }
  #if defined(MY_DEBUG)
  JoinProbingTime += buildAndProbe.get();
  #endif
}
//---------------------------------------------------------------------------
bool SMJoin::require(SelectInfo info)
  // Require a column and add it to results
{
  if (requestedColumns.count(info)==0) {
    bool success=false;
    if(left->require(info)) {
      requestedColumnsLeft.emplace_back(info);
      success=true;
    } else if (right->require(info)) {
      success=true;
      requestedColumnsRight.emplace_back(info);
    }
    if (!success)
      return false;

    tmpResults.emplace_back();
    if (isRoot) {
      tmpSums.emplace_back(0UL);
    }
    requestedColumns.emplace(info);
  }
  return true;
}
//---------------------------------------------------------------------------
void SMJoin::copy2Result(uint64_t leftId,uint64_t rightId)
  // Copy to result
{
  unsigned relColId=0;
  for (unsigned cId=0;cId<copyLeftData.size();++cId) {
    if (isRoot) {
      tmpSums[relColId] += copyLeftData[cId][leftId];
    }
    tmpResults[relColId++].push_back(copyLeftData[cId][leftId]);
  }

  for (unsigned cId=0;cId<copyRightData.size();++cId) {
    if (isRoot) {
      tmpSums[relColId] += copyRightData[cId][rightId];
    }
    tmpResults[relColId++].push_back(copyRightData[cId][rightId]);
  }
  ++resultSize;
}
void SMJoin::run()
  // Run
{
  left->require(pInfo.left);
  right->require(pInfo.right);
  left->run();
  right->run();
  #ifdef MY_DEBUG
  Timer jj;
  #endif


  // Use smaller input for build
  if (left->resultSize>right->resultSize) {
    swap(left,right);
    swap(pInfo.left,pInfo.right);
    swap(requestedColumnsLeft,requestedColumnsRight);
  }

  auto leftInputData=left->getResults();
  auto rightInputData=right->getResults();

  // Resolve the input columns
  unsigned resColId=0;
  for (auto& info : requestedColumnsLeft) {
    copyLeftData.push_back(leftInputData[left->resolve(info)]);
    select2ResultColId[info]=resColId++;
  }
  for (auto& info : requestedColumnsRight) {
    copyRightData.push_back(rightInputData[right->resolve(info)]);
    select2ResultColId[info]=resColId++;
  }

  if (left->resultSize == 0 || right -> resultSize == 0) return;
  auto leftColId=left->resolve(pInfo.left);
  auto rightColId=right->resolve(pInfo.right);

  // sort left
  auto leftKeyColumn=leftInputData[leftColId];
  vector<pair<uint64_t, uint64_t> > leftIdx(left->resultSize);
  auto rightKeyColumn=rightInputData[rightColId];
  vector<pair<uint64_t, uint64_t> > rightIdx(right->resultSize);

  for (uint64_t i = 0; i < max(left->resultSize, right->resultSize); ++i) {
    if (i < left->resultSize) leftIdx[i] = {leftKeyColumn[i], i};
    if (i < right->resultSize) rightIdx[i] = {rightKeyColumn[i], i};
  }

  // sort right
  sort(leftIdx.begin(), leftIdx.end());
  sort(rightIdx.begin(), rightIdx.end());
 

  // pair<uint64_t, uint64_t> lRange = {leftIdx.front().first, leftIdx.back().first};
  // pair<uint64_t, uint64_t> rRange = {rightIdx.front().first, rightIdx.back().first};
 
  // auto getIntersection = [](pair<uint64_t, uint64_t>& p1, pair<uint64_t, uint64_t>& p2) {
  //     return make_pair(std::max(p1.first, p2.first), std::min(p1.second, p2.second));
  // };
  // auto range = getIntersection(lRange, rRange);
  // if (range.first > range.second) return;
  uint64_t l = 0, r = 0, ls = 0, rs = 0;
  // while (leftIdx[l].first != range.first) ++l;
  // while (rightIdx[r].first != range.first) ++r;
  while (l < left->resultSize && r < right -> resultSize) {
    while (l < left->resultSize && r < right -> resultSize && leftIdx[l].first < rightIdx[r].first) ++l;
    while (l < left->resultSize && r < right -> resultSize && leftIdx[l].first > rightIdx[r].first) ++r;
    rs = r;
    while (l < left->resultSize && r < right -> resultSize && leftIdx[l].first == rightIdx[r].first) {
      while (l < left->resultSize && r < right -> resultSize && leftIdx[l].first == rightIdx[r].first) {
        copy2Result(leftIdx[l].second, rightIdx[r].second);
        ++r;
      }
      r = rs; 
      ++l;
      // if (l < left->resultSize && leftIdx[l].first > range.second) return; 
    }
  }

  #ifdef MY_DEBUG
  SMJoinTime += jj.get();
  #endif
}
//---------------------------------------------------------------------------
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
//---------------------------------------------------------------------------
bool SelfJoin::require(SelectInfo info)
  // Require a column and add it to results
{
  if (requiredIUs.count(info))
    return true;
  if(input->require(info)) {
    tmpResults.emplace_back();
    if (isRoot) {
      tmpSums.emplace_back(0UL);
    }
    requiredIUs.emplace(info);
    return true;
  }
  return false;
}
//---------------------------------------------------------------------------
void SelfJoin::run()
  // Run
{
  input->require(pInfo.left);
  input->require(pInfo.right);
  input->run();
  inputData=input->getResults();
  #ifdef MY_DEBUG
  Timer t;
  #endif

  for (auto& iu : requiredIUs) {
    auto id=input->resolve(iu);
    copyData.emplace_back(inputData[id]);
    select2ResultColId.emplace(iu,copyData.size()-1);
  }

  auto leftColId=input->resolve(pInfo.left);
  auto rightColId=input->resolve(pInfo.right);

  auto leftCol=inputData[leftColId];
  auto rightCol=inputData[rightColId];
  for (uint64_t i=0;i<input->resultSize;++i) {
    if (leftCol[i]==rightCol[i])
      copy2Result(i);
  }
  #ifdef MY_DEBUG
  SelfJoinTime += t.get();
  #endif
}
//---------------------------------------------------------------------------
void Checksum::run()
  // Run
{
  for (auto& sInfo : colInfo) {
    input->require(sInfo);
  }
  #ifdef MY_DEBUG
  Timer tt;
  #endif
  input->run();
  #ifdef MY_DEBUG
  tt.restart();
  #endif

  #ifdef MY_DEBUG
  #endif
  auto sums=input->getSums();
  #ifdef MY_DEBUG
  tt.restart();
  #endif
  for (auto& sInfo : colInfo) {
    auto colId=input->resolve(sInfo);
    resultSize=input->resultSize;
    checkSums.push_back(sums[colId]);
  }
  #ifdef MY_DEBUG
  #endif
}
//---------------------------------------------------------------------------

vector<uint64_t* > ParallelHashJoin::getResults()
  // Get materialized results
{
  vector<uint64_t* > resultVector(paralleltmpResults[0].size());
  for (uint64_t i = 0; i < resultVector.size(); ++i) {
    resultVector[i] = (uint64_t*)malloc(sizeof(uint64_t) * resultSize);
  }
    for (uint64_t cid = 0; cid < paralleltmpResults[0].size(); ++cid) {
    uint64_t s = 0;
  for (uint64_t tid = 0; tid < workerCnt; ++tid) {
      for (uint64_t id = 0; id < paralleltmpResults[tid][0].size(); ++id) {
        // resultVector[cid].push_back(paralleltmpResults[tid][cid][id]);
        resultVector[cid][s +  id] = paralleltmpResults[tid][cid][id];
      }
      s += paralleltmpResults[tid][0].size();
    }
  }
  return resultVector;
}

bool ParallelHashJoin::require(SelectInfo info)
  // Require a column and add it to results
{
  if (requestedColumns.count(info)==0) {
    bool success=false;
    if(left->require(info)) {
      requestedColumnsLeft.emplace_back(info);
      success=true;
    } else if (right->require(info)) {
      success=true;
      requestedColumnsRight.emplace_back(info);
    }
    if (!success)
      return false;

    tmpResults.emplace_back();
    for (auto& e: paralleltmpResults) {
      e.emplace_back();
    }
    if (isRoot) {
      tmpSums.emplace_back(0UL);
    }
    requestedColumns.emplace(info);
  }
  return true;
}
//---------------------------------------------------------------------------
void ParallelHashJoin::copy2Result(uint64_t tid, uint64_t leftId,uint64_t rightId)
  // Copy to result
{
  unsigned relColId=0;
  for (unsigned cId=0;cId<copyLeftData.size();++cId) {
    if (isRoot) {
      tmpSums[relColId] += copyLeftData[cId][leftId];
    }
    paralleltmpResults[tid][relColId++].push_back(copyLeftData[cId][leftId]);
  }

  for (unsigned cId=0;cId<copyRightData.size();++cId) {
    if (isRoot) {
      tmpSums[relColId] += copyRightData[cId][rightId];
    }
    paralleltmpResults[tid][relColId++].push_back(copyRightData[cId][rightId]);
  }
  ++resultSize;
}
//---------------------------------------------------------------------------
void ParallelHashJoin::run()
  // Run
{
  left->require(pInfo.left);
  right->require(pInfo.right);
  left->run();
  right->run();
  #ifdef MY_DEBUG
  Timer jj;
  #endif


  // Use smaller input for build
  if (left->resultSize>right->resultSize) {
    swap(left,right);
    swap(pInfo.left,pInfo.right);
    swap(requestedColumnsLeft,requestedColumnsRight);
  }

  auto leftInputData=left->getResults();
  auto rightInputData=right->getResults();

  // Resolve the input columns
  unsigned resColId=0;
  for (auto& info : requestedColumnsLeft) {
    copyLeftData.push_back(leftInputData[left->resolve(info)]);
    select2ResultColId[info]=resColId++;
  }
  for (auto& info : requestedColumnsRight) {
    copyRightData.push_back(rightInputData[right->resolve(info)]);
    select2ResultColId[info]=resColId++;
  }

  auto leftColId=left->resolve(pInfo.left);
  auto rightColId=right->resolve(pInfo.right);

  #if defined(MY_DEBUG)
  Timer buildAndProbe;
  #endif // MY_DEBUG
  
  // Build phase
  auto leftKeyColumn=leftInputData[leftColId];
  hashTable.reserve(left->resultSize*2);
  for (uint64_t i=0,limit=i+left->resultSize;i!=limit;++i) {
    hashTable.emplace(leftKeyColumn[i],i);
  }

  #if defined(MY_DEBUG)
  JoinHashBuildTime += buildAndProbe.get();
  buildAndProbe.restart();
  #endif // MY_DEBUG
  // Probe phase
  auto rightKeyColumn=rightInputData[rightColId];

  auto getWorkloadRanges = [](uint64_t total, uint64_t threadNum) {
    vector<pair<uint64_t, uint64_t> > ranges;



  };

  auto getProperWorkerCnt = [](uint64_t totalWorkSize) {
    uint64_t workerSize = totalWorkSize / MIN_WORK_SIZE;
    if (workerSize > JOIN_THREAD_NUM) {
      workerSize = JOIN_THREAD_NUM;
    }
    if (workerSize == 0) workerSize = 1;
    return workerSize;
  };
  workerCnt = getProperWorkerCnt(right->resultSize);
  
  auto workSize = right->resultSize / workerCnt;
  vector<future<void> > futures;
  for (int tid = 0; tid < workerCnt; ++tid) {
    auto s = workSize * tid;
    auto e = workSize * (tid + 1);
    if (tid == workerCnt - 1) e = right->resultSize;
    futures.emplace_back( joinpool.submit([&copyLeftData=copyLeftData, isRoot=isRoot,&paralleltmpResults=paralleltmpResults,&tmpSums=tmpSums,&copyRightData=copyRightData,&resultSize=resultSize, &hashTable = hashTable, &rightKeyColumn, tid, s, e] {
      vector<uint64_t> localSums(copyLeftData.size() + copyRightData.size(), 0);
      uint64_t localResultSize = 0;
      for (auto i = s; i != e; ++i) {
        auto rightKey=rightKeyColumn[i];
        auto range=hashTable.equal_range(rightKey);
        for (auto iter=range.first;iter!=range.second;++iter) {
          // copy2Result(tid, iter->second,i);
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
        for (int i = 0; i < localSums.size(); ++i) {
        __sync_fetch_and_add(&tmpSums[i], localSums[i]);
        }
      }
      __sync_fetch_and_add(&resultSize, localResultSize);
    })
    );
  }
  for (auto&& f: futures) {
    f.wait();
  }
  // joinpool.wait_for_tasks();
  // for (uint64_t i=0,limit=i+right->resultSize;i!=limit;++i) {
  //   auto rightKey=rightKeyColumn[i];
  //   auto range=hashTable.equal_range(rightKey);
  //   for (auto iter=range.first;iter!=range.second;++iter) {
  //     copy2Result(iter->second,i);
  //   }
  // }
  #if defined(MY_DEBUG)
  JoinProbingTime += buildAndProbe.get();
  #endif
}
 
