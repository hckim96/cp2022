#include <gtest/gtest.h>
#include <unordered_map>
#include "Timer.hpp"

TEST(TimerTest, HandlesTimer) {
  std::vector<double> checkList = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  // check timer can estimate time with 20% error
  for (auto time: checkList) {
    Timer timer;
    while (true) {
      if (timer.get() >= time) break;
    }
    auto timerTime = timer.get();
    
    ASSERT_GE(timerTime, (time) * 0.8);
    ASSERT_LE(timerTime, (time) * 1.2);
  }
}