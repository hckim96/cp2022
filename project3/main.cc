#include <algorithm>
#include <random>
#include <vector>
#include <string>
#include <iostream>

#include "bwtree.h"
#include "bwtree_test_util.h"
#include "multithread_test_util.h"
#include "timer.h"
#include "worker_pool.h"
#include "zipf.h"


using namespace std;

int main(int argc, char *argv[]) {
#if 0
  double alpha = 3.0;
  std::vector<int> count(11, 0);

  rand_val(alpha);
  for (int i = 0; i < 10000; i++) {
    count[zipf(alpha, 10)]++;
  }

  std::sort(count.begin(), count.end());

  std::cout << "Ratio for alpha: " << alpha << std::endl;
  for (int i = 1; i < 11; i++) {
    std::cout << count[i] << std::endl;
  }
#endif
  #if !defined(MY_DEBUG)

  const uint32_t num_threads_ =
    test::MultiThreadTestUtil::HardwareConcurrency() + (test::MultiThreadTestUtil::HardwareConcurrency() % 2);

  // This defines the key space (0 ~ (1M - 1))
  const uint32_t key_num = 1024 * 1024* 4;

  common::WorkerPool thread_pool(num_threads_, {});
  thread_pool.Startup();

  std::vector<int> leaf_uppder_threshold_test_params{
    128,
    256,
    512,
    1024,
    2048,
    3072,
    3584,
  };
  int each_test_cnt = 10;
  // leaf upper threshold, keyNum * Insert time, total af_time (af: alloc + free), total af_cnt, total af_memory, mutextime
  vector<tuple<int, double, double, uint64_t, uint64_t, double> > testresults;
  vector<tuple<int, double, double, uint64_t, uint64_t, double> > testresults_avg;
  for (auto leaf_upper_threashold_test_param: leaf_uppder_threshold_test_params) {
    double insertlat_total = 0;
    double mallocfreelat_total = 0;
    uint64_t af_cnt_total = 0;
    uint64_t af_memory_total = 0;
    double mtxTime_total = 0;
    for (int i = 0; i < each_test_cnt; ++i) {
      std::atomic<size_t> insert_success_counter = 0;
      std::atomic<size_t> total_op_counter = 0;

      auto *const tree = test::BwTreeTestUtil::GetEmptyTree();
      tree->SetLeafNodeSizeUpperThreshold(leaf_upper_threashold_test_param);

      auto workload = [&](uint32_t id) {
        const uint32_t gcid = id + 1;
        tree->AssignGCID(gcid);
        std::default_random_engine thread_generator(id);
        std::uniform_int_distribution<int> uniform_dist(0, key_num - 1);
        uint32_t op_cnt = 0;

        while (insert_success_counter.load() < key_num) {
          int key = uniform_dist(thread_generator);

          // util::Timer<std::milli> insertTimer;
          if (tree->Insert(key, key)) insert_success_counter.fetch_add(1);
          // auto tmp = insertTimer.GetElapsed();
          op_cnt++;
        }
        tree->UnregisterThread(gcid);
        total_op_counter.fetch_add(op_cnt);
      };

      util::Timer<std::milli> timer;
      timer.Start();

      tree->UpdateThreadLocal(num_threads_ + 1);
      test::MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads_, workload);
      tree->UpdateThreadLocal(1);

      timer.Stop();

      double insertTotal = timer.GetElapsed();

      std::cout << "testparam: " << leaf_upper_threashold_test_param << '\n';
      double ops = total_op_counter.load() / (timer.GetElapsed() / 1000.0);
      double success_ops = insert_success_counter.load() / (timer.GetElapsed() / 1000.0);
      std::cout << std::fixed << key_num / 1024 / 1024 << "M Insert(): " << timer.GetElapsed() << " (ms), "
        << "write throughput: " << ops << " (op/s), "
        << "successive write throughput: " << success_ops << " (op/s)" << std::endl;
      std::cout << "tree->af_time_total: " << tree->af_time_total << '\n';
      std::cout << "tree->af_cnt_total: " << tree->af_cnt_total << '\n';
      std::cout << "tree->af_memory_total: " << tree->af_memory_total << '\n';
      std::cout << "tree->mtxtime: " << tree->mftimeMutxTime << '\n';

      timer.Start();
      // Verifies whether random insert is correct
      for (uint32_t i = 0; i < key_num; i++) {
        auto s = tree->GetValue(i);

        assert(s.size() == 1);
        assert(*s.begin() == i);
      }
      timer.Stop();

      double latency =  (timer.GetElapsed() / key_num);
      // std::cout << num_threads_ << '\n';
      std::cout << std::fixed << key_num / 1024 / 1024 << "M Get(): " << timer.GetElapsed() << " (ms), "
        << "avg read latency: " << latency << " (ms) " << std::endl;

      testresults.push_back({leaf_upper_threashold_test_param, insertTotal, tree->af_time_total, tree->af_cnt_total, tree->af_memory_total, tree->mftimeMutxTime});
      insertlat_total += insertTotal;
      mallocfreelat_total += tree->af_time_total;
      af_cnt_total += tree -> af_cnt_total;
      af_memory_total += tree -> af_memory_total;
      delete tree;
    }
    testresults_avg.push_back({leaf_upper_threashold_test_param, insertlat_total / each_test_cnt, mallocfreelat_total / each_test_cnt, af_cnt_total / each_test_cnt, af_memory_total / each_test_cnt, mtxTime_total / each_test_cnt});
  }

  std::cout << "testresults\n";
  for (auto [t, l, m, cnt, memory, mtx]: testresults) {
    std::cout << t << '\t' << l << '\t' << m << '\t' << cnt << '\t' << memory <<  '\t' << mtx << '\n';
  }
  std::cout << "testresults_avg\n";
  for (auto [t, l, m, cnt, memory, mtx]: testresults_avg) {
    std::cout << t << '\t' << l << '\t' << m << '\t' << cnt << '\t' << memory <<  '\t' << mtx << '\n';
  }
  // Inserts in a 1M key space randomly until all keys has been inserted

  // vector


  #endif // MY_DEBUG

  #if defined(MY_DEBUG)
  auto *const tree = test::BwTreeTestUtil::GetEmptyTree();

  for (int i = 1; i <= 3; ++i) {
    auto ret = tree->Insert(i, i);
    std::cout << i << "(ret): " << ret << '\n';
  }
  // tree->Insert(1, 1);
  // tree->Insert(2, 2);
  // tree->Insert(3, 3);
  auto s = tree->GetValue(1);
  s = tree->GetValue(4);
  delete tree;
  #endif // MY_DEBUG
  
  return 0;
}
