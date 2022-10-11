#include <gtest/gtest.h>
#include <unordered_map>
#include "RandomIntGenerator.hpp"

TEST(RandomIntGeneratorTest, HandlesUniformDistribution) {

  long long randomCnt = 10000;
  RandomIntGenerator rg;
  long long kind = 0;
  std::unordered_map<int, int> freq;
  for (int i = 0; i < randomCnt; ++i) {
    if (freq[i]++ == 0) {
      ++kind;
    };
  }

  long long eachCnt = randomCnt / kind;
  for (auto [num, cnt]: freq) {
    // to make uniform distribution... cnt is always eachCnt or eachCnt + 1.
    // if randomCnt <= 1 << 32 (number of int32), cnt will be always 1.
    ASSERT_EQ(cnt, 1);
  }
}