#include <gtest/gtest.h>
#include <unordered_map>
#include "Timer.hpp"

TEST(TimerTest, HandlesTimer) {
  std::vector<double> checkList = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  // check timer can estimate time with 20% error
  for (auto time: checkList) {
    Timer timer;
    struct timeval t;
    
    gettimeofday(&t, 0);
    auto start = t.tv_sec + t.tv_usec / 1000000.0;
    while (true) {
      if (timer.get() >= time) break;
    }
    gettimeofday(&t, 0);
    auto end = t.tv_sec + t.tv_usec / 1000000.0;
    auto elapsed_time = end - start;
    
    ASSERT_GE(elapsed_time, (time) * 0.8);
    ASSERT_LE(elapsed_time, (time) * 1.2);
  }
}