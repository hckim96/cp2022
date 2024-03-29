#pragma once
#include <cassert>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <set>
#include "Relation.hpp"
#include "Parser.hpp"
#include "../BS_thread_pool.hpp"
#include <map>
#include "debug.hpp"
#include "timer.hpp"

//---------------------------------------------------------------------------
namespace std {
  /// Simple hash function to enable use with unordered_map
  template<> struct hash<SelectInfo> {
    std::size_t operator()(SelectInfo const& s) const noexcept { return s.binding ^ (s.colId << 5); }
  };
};
//---------------------------------------------------------------------------
class Operator {
  /// Operators materialize their entire result

  protected:
  /// Mapping from select info to data
  std::unordered_map<SelectInfo,unsigned> select2ResultColId;
  /// The materialized results
  std::vector<uint64_t*> resultColumns;
  /// The tmp results
  std::vector<std::vector<uint64_t>> tmpResults;
  /// The tmp results of threads [tid][colId][rowId] -> val
  std::vector<std::vector<std::vector<uint64_t>>> paralleltmpResults;
  /// The tmp sums
  std::vector<uint64_t> tmpSums;


  public:

  unsigned workerCnt;

  /// is root of the tree(if true, calculate sum while probing)
  bool isRoot=false;
  /// Require a column and add it to results
  virtual bool require(SelectInfo info) = 0;
  /// Resolves a column
  unsigned resolve(SelectInfo info) { assert(select2ResultColId.find(info)!=select2ResultColId.end()); return select2ResultColId[info]; }
  /// Run
  virtual void run() = 0;
  /// Get  materialized results
  virtual std::vector<uint64_t*> getResults();
  /// Get  materialized results
  virtual std::vector<uint64_t> getSums();
  /// The result size
  uint64_t resultSize=0;
  /// The destructor
  virtual ~Operator() {};
};
//---------------------------------------------------------------------------
class Scan : public Operator {
  protected:
  /// The relation
  Relation& relation;
  /// The name of the relation in the query
  unsigned relationBinding;

  public:
  /// The constructor
  Scan(Relation& r,unsigned relationBinding) : relation(r), relationBinding(relationBinding) {};
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run
  void run() override;
  /// Get  materialized results
  virtual std::vector<uint64_t* > getResults() override;
  /// Get  materialized results
  virtual std::vector<uint64_t> getSums() override {return Operator::getSums();};
};
//---------------------------------------------------------------------------
class FilterScan : public Scan {
  /// The filter info
  std::vector<FilterInfo> filters;
  /// The input data
  std::vector<uint64_t*> inputData;
  /// Apply filter
  bool applyFilter(uint64_t id,FilterInfo& f);
  /// Copy tuple to result
  void copy2Result(uint64_t id);

  public:
  /// The constructor
  FilterScan(Relation& r,std::vector<FilterInfo> filters) : Scan(r,filters[0].filterColumn.binding), filters(filters)  {paralleltmpResults.resize(FSSCAN_THREAD_NUM);};
  /// The constructor
  FilterScan(Relation& r,FilterInfo& filterInfo) : FilterScan(r,std::vector<FilterInfo>{filterInfo}) {paralleltmpResults.resize(FSSCAN_THREAD_NUM);};
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run
  void run() override;
  /// Get  materialized results
  virtual std::vector<uint64_t*> getResults() override;
  /// Get  materialized results
  virtual std::vector<uint64_t> getSums() override {return Operator::getSums();};
};
//---------------------------------------------------------------------------
class Join : public Operator {
  /// The input operators
  std::unique_ptr<Operator> left, right;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// Copy tuple to result
  void copy2Result(uint64_t leftId,uint64_t rightId);
  /// Create mapping for bindings
  void createMappingForBindings();

  using HT=std::unordered_multimap<uint64_t,uint64_t>;

  /// The hash table for the join
  HT hashTable;
  /// Columns that have to be materialized
  std::unordered_set<SelectInfo> requestedColumns;
  /// Left/right columns that have been requested
  std::vector<SelectInfo> requestedColumnsLeft,requestedColumnsRight;


  /// The entire input data of left and right
  std::vector<uint64_t*> leftInputData,rightInputData;
  /// The input data that has to be copied
  std::vector<uint64_t*>copyLeftData,copyRightData;

  public:
  /// tmp
  bool isSelf=false;

  /// The constructor
  Join(std::unique_ptr<Operator>&& left,std::unique_ptr<Operator>&& right,PredicateInfo& pInfo) : left(std::move(left)), right(std::move(right)), pInfo(pInfo) {};
  /// The constructor2
  Join(std::unique_ptr<Operator>&& left,std::unique_ptr<Operator>&& right,PredicateInfo& pInfo, bool isSelf) : left(std::move(left)), right(std::move(right)), pInfo(pInfo), isSelf(isSelf) {};
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run
  void run() override;

  /// Get  materialized results
  virtual std::vector<uint64_t> getSums() override {return Operator::getSums();};
};
class SMJoin : public Operator {
  /// The input operators
  std::unique_ptr<Operator> left, right;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// Copy tuple to result
  void copy2Result(uint64_t leftId,uint64_t rightId);
  /// Create mapping for bindings
  void createMappingForBindings();

  using HT=std::unordered_multimap<uint64_t,uint64_t>;

  /// The hash table for the join
  HT hashTable;
  /// Columns that have to be materialized
  std::unordered_set<SelectInfo> requestedColumns;
  /// Left/right columns that have been requested
  std::vector<SelectInfo> requestedColumnsLeft,requestedColumnsRight;


  /// The entire input data of left and right
  std::vector<uint64_t*> leftInputData,rightInputData;
  /// The input data that has to be copied
  std::vector<uint64_t*>copyLeftData,copyRightData;

  public:
  /// tmp
  bool isSelf=false;

  /// The constructor
  SMJoin(std::unique_ptr<Operator>&& left,std::unique_ptr<Operator>&& right,PredicateInfo& pInfo) : left(std::move(left)), right(std::move(right)), pInfo(pInfo) {};
  /// The constructor2
  SMJoin(std::unique_ptr<Operator>&& left,std::unique_ptr<Operator>&& right,PredicateInfo& pInfo, bool isSelf) : left(std::move(left)), right(std::move(right)), pInfo(pInfo), isSelf(isSelf) {};
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run
  void run() override;

  /// Get  materialized results
  virtual std::vector<uint64_t> getSums() override {return Operator::getSums();};
};
//---------------------------------------------------------------------------
class SelfJoin : public Operator {
  /// The input operators
  std::unique_ptr<Operator> input;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// Copy tuple to result
  void copy2Result(uint64_t id);
  /// The required IUs
  std::set<SelectInfo> requiredIUs;

  /// The entire input data
  std::vector<uint64_t*> inputData;
  /// The input data that has to be copied
  std::vector<uint64_t*>copyData;

  public:
  /// The constructor
  SelfJoin(std::unique_ptr<Operator>&& input,PredicateInfo& pInfo) : input(std::move(input)), pInfo(pInfo) {};
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run
  void run() override;
  /// Get  materialized results
  virtual std::vector<uint64_t> getSums() override {return Operator::getSums();};
};
//---------------------------------------------------------------------------
class Checksum : public Operator {
  /// The input operator
  std::unique_ptr<Operator> input;
  /// The join predicate info
  std::vector<SelectInfo>& colInfo;
  /// map sinfo to it's sum
  // std::unordered_map<SelectInfo,uint64_t> checksumCache;


  public:
  std::vector<uint64_t> checkSums;
  /// The constructor
  Checksum(std::unique_ptr<Operator>&& input,std::vector<SelectInfo>& colInfo) : input(std::move(input)), colInfo(colInfo) {};
  /// Request a column and add it to results
  bool require(SelectInfo info) override { throw; /* check sum is always on the highest level and thus should never request anything */ }
  /// Run
  void run() override;
};
//---------------------------------------------------------------------------


//---------------------------------------------------------------------------
class ParallelHashJoin : public Operator {
  /// The input operators
  std::unique_ptr<Operator> left, right;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// Copy tuple to result
  // void copy2Result(uint64_t leftId,uint64_t rightId);
  void copy2Result(uint64_t, uint64_t leftId,uint64_t rightId);
  /// Create mapping for bindings
  void createMappingForBindings();

  using HT=std::unordered_multimap<uint64_t,uint64_t>;

  /// The hash table for the join
  HT hashTable;
  /// Columns that have to be materialized
  std::unordered_set<SelectInfo> requestedColumns;
  /// Left/right columns that have been requested
  std::vector<SelectInfo> requestedColumnsLeft,requestedColumnsRight;


  /// The entire input data of left and right
  std::vector<uint64_t*> leftInputData,rightInputData;
  /// The input data that has to be copied
  std::vector<uint64_t*>copyLeftData,copyRightData;

  public:
  /// tmp
  bool isSelf=false;

  /// The constructor
  ParallelHashJoin(std::unique_ptr<Operator>&& left,std::unique_ptr<Operator>&& right,PredicateInfo& pInfo) : left(std::move(left)), right(std::move(right)), pInfo(pInfo) {paralleltmpResults.resize(JOIN_THREAD_NUM);};
  /// The constructor2
  ParallelHashJoin(std::unique_ptr<Operator>&& left,std::unique_ptr<Operator>&& right,PredicateInfo& pInfo, bool isSelf) : left(std::move(left)), right(std::move(right)), pInfo(pInfo), isSelf(isSelf) {paralleltmpResults.resize(JOIN_THREAD_NUM);};
  /// Require a column and add it to results
  /*
    merge the parallel tmp results into one.
  */
  std::vector<uint64_t*> getResults() override;
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run

  void run() override;

  /// Get  materialized results
  virtual std::vector<uint64_t> getSums() override {return Operator::getSums();};
};
//---------------------------------------------------------------------------
class ParallelSelfJoin : public Operator {
  /// The input operators
  std::unique_ptr<Operator> input;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// Copy tuple to result
  void copy2Result(uint64_t id);
  /// The required IUs
  std::set<SelectInfo> requiredIUs;

  /// The entire input data
  std::vector<uint64_t*> inputData;
  /// The input data that has to be copied
  std::vector<uint64_t*>copyData;

  unsigned workerCnt;

  public:
  /// The constructor
  ParallelSelfJoin(std::unique_ptr<Operator>&& input,PredicateInfo& pInfo) : input(std::move(input)), pInfo(pInfo) {};
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run
  void run() override;
  /// Get  materialized results
  virtual std::vector<uint64_t> getSums() override {return Operator::getSums();};
};