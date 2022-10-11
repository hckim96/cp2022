#pragma once
#include <random>

class RandomIntGenerator
{
private:
  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<int> dis;
public:
  RandomIntGenerator(): gen(rd()), dis(std::uniform_int_distribution<int>(INT32_MIN, INT32_MAX)) {};
  int generate() {
    return dis(gen);
  }
};
