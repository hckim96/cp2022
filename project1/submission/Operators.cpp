#include <Operators.hpp>
#include <cassert>
#include <iostream>
#include <map>
#include "ThreadPool.h"
//---------------------------------------------------------------------------
using namespace std;
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
    unsigned colId=tmpResults.size()-1;
    select2ResultColId[info]=colId;
  }
  return true;
}
//---------------------------------------------------------------------------
void FilterScan::copy2Result(uint64_t id)
  // Copy to result
{
  for (unsigned cId=0;cId<inputData.size();++cId)
    tmpResults[cId].push_back(inputData[cId][id]);
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
    if (emptyResult) return;
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
    if (intersection.first > intersection.second) return;
    for (int j = 0; j < filters.size(); ++j) {
      if (i == j) continue;
      if (filters[i].filterColumn == filters[j].filterColumn) {
        // rel, col same
        intersection = applyFilterToRange(intersection, filters[j]);
        if (intersection.first > intersection.second) return;
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
}
//---------------------------------------------------------------------------
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
    requestedColumns.emplace(info);
  }
  return true;
}
//---------------------------------------------------------------------------
void Join::copy2Result(uint64_t leftId,uint64_t rightId)
  // Copy to result
{
  unsigned relColId=0;
  for (unsigned cId=0;cId<copyLeftData.size();++cId)
    tmpResults[relColId++].push_back(copyLeftData[cId][leftId]);

  for (unsigned cId=0;cId<copyRightData.size();++cId)
    tmpResults[relColId++].push_back(copyRightData[cId][rightId]);
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

  // Build phase
  auto leftKeyColumn=leftInputData[leftColId];
  hashTable.reserve(left->resultSize*2);
  for (uint64_t i=0,limit=i+left->resultSize;i!=limit;++i) {
    hashTable.emplace(leftKeyColumn[i],i);
  }
  // Probe phase
  auto rightKeyColumn=rightInputData[rightColId];
  for (uint64_t i=0,limit=i+right->resultSize;i!=limit;++i) {
    auto rightKey=rightKeyColumn[i];
    auto range=hashTable.equal_range(rightKey);
    for (auto iter=range.first;iter!=range.second;++iter) {
      copy2Result(iter->second,i);
    }
  }
}
//---------------------------------------------------------------------------
void SelfJoin::copy2Result(uint64_t id)
  // Copy to result
{
  for (unsigned cId=0;cId<copyData.size();++cId)
    tmpResults[cId].push_back(copyData[cId][id]);
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
}
//---------------------------------------------------------------------------
void Checksum::run()
  // Run
{
  for (auto& sInfo : colInfo) {
    input->require(sInfo);
  }
  input->run();
  auto results=input->getResults();

  for (auto& sInfo : colInfo) {
    // if (checksumCache.count(sInfo)) {
    //   checkSums.push_back(checksumCache[sInfo]);
    //   continue;
    // }
    auto colId=input->resolve(sInfo);
    auto resultCol=results[colId];
    uint64_t sum=0;
    resultSize=input->resultSize;
    for (auto iter=resultCol,limit=iter+input->resultSize;iter!=limit;++iter)
      sum+=*iter;
    
    // checksumCache[sInfo] = sum;
    checkSums.push_back(sum);
  }
}
//---------------------------------------------------------------------------
