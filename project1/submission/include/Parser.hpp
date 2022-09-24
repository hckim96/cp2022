#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>
#include <set>

#include "Relation.hpp"
//---------------------------------------------------------------------------
struct SelectInfo {
   /// Relation id
   RelationId relId;
   /// Binding for the relation
   unsigned binding;
   /// Column id
   unsigned colId;
   /// The constructor
   SelectInfo(RelationId relId,unsigned b,unsigned colId) : relId(relId), binding(b), colId(colId) {};
   /// The constructor if relation id does not matter
   SelectInfo(unsigned b,unsigned colId) : SelectInfo(-1,b,colId) {};

   /// Equality operator
   inline bool operator==(const SelectInfo& o) const {
     return o.relId == relId && o.binding == binding && o.colId == colId;
   }
   /// Less Operator
   inline bool operator<(const SelectInfo& o) const {
   //   return binding<o.binding||colId<o.colId;
      if (binding < o.binding) {
         return true;
      } else if (binding == o.binding) {
         if (colId < o.colId) {
            return true;
         }
      }
      return false;
   }

   /// Dump text format
   std::string dumpText();
   /// Dump SQL
   std::string dumpSQL(bool addSUM=false);

   /// The delimiter used in our text format
   static const char delimiter=' ';
   /// The delimiter used in SQL
   constexpr static const char delimiterSQL[]=", ";
};
//---------------------------------------------------------------------------
struct FilterInfo {
   enum Comparison : char { Less='<', Greater='>', Equal='=' };
   /// Filter Column
   SelectInfo filterColumn;
   /// Constant
   uint64_t constant;
   /// Comparison type
   Comparison comparison;
   /// Dump SQL
   std::string dumpSQL();

   /// The constructor
   FilterInfo(SelectInfo filterColumn,uint64_t constant,Comparison comparison) : filterColumn(filterColumn), constant(constant), comparison(comparison) {};
   /// Dump text format
   std::string dumpText();

   /// The delimiter used in our text format
   static const char delimiter='&';
   /// The delimiter used in SQL
   constexpr static const char delimiterSQL[]=" and ";
};
static const std::vector<FilterInfo::Comparison> comparisonTypes { FilterInfo::Comparison::Less, FilterInfo::Comparison::Greater, FilterInfo::Comparison::Equal};
//---------------------------------------------------------------------------
struct PredicateInfo {
   /// Left
   SelectInfo left;
   /// Right
   SelectInfo right;
   /// The constructor
   PredicateInfo(SelectInfo left, SelectInfo right) : left(left), right(right){};
   /// Dump text format
   std::string dumpText();
   /// Dump SQL
   std::string dumpSQL();

   /// The delimiter used in our text format
   static const char delimiter='&';
   /// The delimiter used in SQL
   constexpr static const char delimiterSQL[]=" and ";
};
//---------------------------------------------------------------------------
class QueryInfo {
   public:
   /// The relation ids
   std::vector<RelationId> relationIds;
   /// The predicates
   std::vector<PredicateInfo> predicates;
   /// The filters
   std::vector<FilterInfo> filters;
   /// The selections
   std::vector<SelectInfo> selections;

   std::vector<std::set<SelectInfo> > same;
   /// Reset query info
   void clear();

   private:
   /// Parse a single predicate
   void parsePredicate(std::string& rawPredicate);
   /// Resolve bindings of relation ids
   void resolveRelationIds();

   public:
   /// Parse relation ids <r1> <r2> ...
   void parseRelationIds(std::string& rawRelations);
   /// Parse predicates r1.a=r2.b&r1.b=r3.c...
   void parsePredicates(std::string& rawPredicates);
   /// Parse selections r1.a r1.b r3.c...
   void parseSelections(std::string& rawSelections);
   /// Parse selections [RELATIONS]|[PREDICATES]|[SELECTS]
   void parseQuery(std::string& rawQuery);

   void sameSelect();

   void sameBinding();

   void addMoreFilterWithPredicates();
   
   /*
      given query 0.1 = 1.0 and 0.1 < 3000 
      where range(1.0) is [2000, 10000] and range(0.1) is [0, 5000]
      then filter 0.1 > 2000 and 1.0 < 5000 can be added
   */
   void addFilterWithPredicateAndColRange();

   /*
      change order of predicates to apply filter scan first rather than scan
   */
   void sortPredicates();


   void removeJoin();
   
   /// Dump text format
   std::string dumpText();
   /// Dump SQL
   std::string dumpSQL();
   /// The empty constructor
   QueryInfo() {}
   /// The constructor that parses a query
   QueryInfo(std::string rawQuery);
};
//---------------------------------------------------------------------------
