#include <Operators.hpp>
#include <cassert>
#include <iostream>
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
extern uint64_t fsNumThread;
extern ThreadPool pool;
void FilterScan::run()
  // Run
{
  vector<future<vector<uint64_t> > > results;

  uint64_t workCnt = relation.size / fsNumThread;

  for (uint64_t tid = 0; tid < fsNumThread; ++tid) {
    uint64_t start = tid * workCnt;
    uint64_t end = start + workCnt;
    if (tid == fsNumThread - 1) end = relation.size;

    results.emplace_back(
      pool.enqueue([this, start, end] {
        vector<uint64_t> res;
        for (uint64_t i = start; i < end; ++i) {
          bool pass = true;
          for (auto&f : filters) {
            pass&=applyFilter(i, f);
          }
          if (pass)
            res.push_back(i);
        }
        return res;
      })
    );
  }

  for (auto && result: results) {
    for (auto i: result.get()) {
      copy2Result(i);
    }
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

  vector<future<uint64_t> > sums;
  int idx = 0;
  for (auto& sInfo : colInfo) {
    sums.emplace_back(
      pool.enqueue([&]{
        auto colId=input->resolve(sInfo);
        auto resultCol=results[colId];
        resultSize = input->resultSize;
        uint64_t sum=0;
        vector<future<uint64_t> > ssums;
        int iidx = 0;

        uint64_t iidxEnd = 7;
        for (uint64_t iidx = 0; iidx < iidxEnd; ++iidx) {
          uint64_t div = input->resultSize / iidxEnd;
          ssums.emplace_back(
            pool.enqueue([=]{
              uint64_t ssum = 0;
              auto s = resultCol;
              s += iidx * div;
              auto e = s + div;
              if (iidx == iidxEnd - 1) e = resultCol + input -> resultSize;
              for (auto iter = s; iter != e; ++iter) {
                ssum += *iter;
              }
              return ssum;
            })
          );
        }
        for (auto&& e: ssums) {
          sum += e.get();
        }
        // resultSize=input->resultSize;
        // for (auto iter=resultCol,limit=iter+input->resultSize;iter!=limit;++iter)
        //   sum+=*iter;
        // checkSums.push_back(sum);
        return sum;
      })
    );
    ++idx;
    // auto colId=input->resolve(sInfo);
    // auto resultCol=results[colId];
    // uint64_t sum=0;
    // resultSize=input->resultSize;

    // for (auto iter=resultCol,limit=iter+input->resultSize;iter!=limit;++iter)
    //   sum+=*iter;
    // checkSums.push_back(sum);
  }
  for (auto && sum: sums) {
    checkSums.push_back(sum.get());
  }
}
//---------------------------------------------------------------------------
