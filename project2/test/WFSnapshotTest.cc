#include <gtest/gtest.h>
#include "WFSnapshot.hpp"
#include "RandomIntGenerator.hpp"
#include <vector>

TEST(WFSnapshotTest, HandlesInit) {
  int thread_num = 10;
  int init = 0;
  WFSnapshot<int> wfsnapshot(thread_num, init);

  // check initialization
  ASSERT_EQ(wfsnapshot.a_table.size(), thread_num);
  for (int i = 0; i < thread_num; ++i) {
    ASSERT_EQ(wfsnapshot.a_table[i].value, init);
  }
}

TEST(WFSnapshotTest, HandlesUpdate) {
  int thread_num = 10;
  int init = 0;
  WFSnapshot<int> wfsnapshot(thread_num, init);

  // update each thread's value to random value
  std::vector<int> randomVals;
  RandomIntGenerator rg;
  auto before = wfsnapshot.a_table;
  for (int i = 0; i < thread_num; ++i) {
    randomVals.push_back(rg.generate());
  }
  for (int i = 0; i < thread_num; ++i) {
    wfsnapshot.update(i, randomVals[i]);
  }

  // check updated
  for (int i = 0; i < thread_num; ++i) {
    ASSERT_EQ(wfsnapshot.a_table[i].stamp, before[i].stamp + 1);
    ASSERT_EQ(wfsnapshot.a_table[i].value, randomVals[i]);
  }
}

TEST(WFSnapshotTest, HandlesCollect) {
  int thread_num = 10;
  int init = 0;
  WFSnapshot<int> wfsnapshot(thread_num, init);

  // update each thread's value to random value
  std::vector<int> randomVals;
  RandomIntGenerator rg;
  for (int i = 0; i < thread_num; ++i) {
    randomVals.push_back(rg.generate());
  }
  for (int i = 0; i < thread_num; ++i) {
    wfsnapshot.update(i, randomVals[i]);
  }
  
  // check collect
  auto collectRes = wfsnapshot.collect();
  for (int i = 0; i < thread_num; ++i) {
    ASSERT_EQ(collectRes[i].stamp, wfsnapshot.a_table[i].stamp);
    ASSERT_EQ(collectRes[i].value, wfsnapshot.a_table[i].value);
    ASSERT_EQ(collectRes[i].snap.size(), wfsnapshot.a_table[i].snap.size());
    for (int j = 0; j < collectRes[i].snap.size(); ++j) {
      ASSERT_EQ(collectRes[i].snap[j], wfsnapshot.a_table[i].snap[j]);
    }
  }
}

TEST(WFSnapshotTest, HandlesScan) {
  int thread_num = 10;
  int init = 0;
  WFSnapshot<int> wfsnapshot(thread_num, init);

  // update each thread's value to random value
  std::vector<int> randomVals;
  RandomIntGenerator rg;
  for (int i = 0; i < thread_num; ++i) {
    randomVals.push_back(rg.generate());
  }
  for (int i = 0; i < thread_num; ++i) {
    wfsnapshot.update(i, randomVals[i]);
  }

  // check scan
  auto scanRes = wfsnapshot.scan();
  for (int i = 0; i < thread_num; ++i) {
    ASSERT_EQ(scanRes[i], randomVals[i]);
  }
}
