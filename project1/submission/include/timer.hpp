#pragma once
#include <sys/time.h>
#include <mutex>
#include <iostream>
#include <debug.hpp>

extern std::mutex cerrMutex;
class Timer {
private:
  double now() {
    struct timeval t;
    gettimeofday(&t, 0);
    return t.tv_sec + t.tv_usec / 1000000.0;
  }
public:
  double s;
  Timer() {
    s = now();
  }

  void restart() {
    s = now();
  }

  double get() {
    return now() - s;
  }

  void cerrget(const char* prefix) {
#ifdef MY_DEBUG
    cerrMutex.lock();
    std::cerr << prefix << get() * 1000 << '\n';
    cerrMutex.unlock();
#endif

  }
};
