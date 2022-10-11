#pragma once
#include <vector>

template <typename T>
class StampedSnap
{
public:
  long long stamp;
  T value;
  std::vector<T> snap;

  StampedSnap(T value_) {
    stamp = 0;
    value = value_;
  };
  StampedSnap(long long stamp_, T value_, std::vector<T> snap_) {
    stamp = stamp_;
    value = value_;
    snap = snap_;
  };
};


template <typename T>
class WFSnapshot
{
// private:

public:
  // public for testing
  std::vector<StampedSnap<T> > a_table;

  // public for testing
  std::vector<StampedSnap<T> > collect() {
    auto copy = a_table;
    return copy;
  };

  WFSnapshot(int capacity, T init) {
    a_table.reserve(capacity);
    for (int i = 0; i < capacity; ++i) {
      a_table.push_back(StampedSnap<T>(init));
      a_table[i].snap.resize(capacity);
    }
  };

  std::vector<T> scan() {
    std::vector<StampedSnap<T> > oldCopy;
    std::vector<StampedSnap<T> > newCopy;
    std::vector<bool> moved(a_table.size(), false);
    oldCopy = collect();

    collect: while (true) {
      newCopy = collect();
      for (int j = 0; j < a_table.size(); ++j) {
        if (oldCopy[j].stamp != newCopy[j].stamp) {
          if (moved[j]) {
            return oldCopy[j].snap;
          } else {
            moved[j] = true;
            oldCopy = newCopy;
            goto collect;
          }
        }
      }
      break;
    }

    std::vector<T> result(a_table.size());
    for (int j = 0; j < a_table.size(); ++j) {
      result[j] = newCopy[j].value;
    }
    return result;
  };

  void update(int me, T value) {
    auto snap = scan();
    auto oldValue = a_table[me];
    auto newValue = StampedSnap<T>(oldValue.stamp + 1, value, snap);
    a_table[me] = newValue;
  };
};