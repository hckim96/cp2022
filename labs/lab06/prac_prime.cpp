#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <iostream>
#include <memory>

#define NUM_THREAD_IN_POOL 4
#define NUM_RESULT_THREAD_IN_POOL 1

boost::asio::io_service io; // worker thread
boost::asio::io_service io_result; // printer thread

bool IsPrime(int n) {
  if (n < 2) {
    return false;
  }

  for (int i = 2; i <= sqrt(n); i++) {
    if (n % i == 0) {
      return false;
    }
  }
  return true;
}

void PrintResult(int start, int end, int result, int seq_num) {
  std::cout << "(" << seq_num << ")"
            << "number of primes in " << start << " ~ " << end << " is "
            << result << std::endl;
}

void CntPrimeInRange(int start, int end, int seq_num) {
  int result = 0;
  for (int i = start; i <= end; i++) {
    if (IsPrime(i)) {
      result++;
    }
  }

  // std::cout << start << "~" << end << "(" << seq_num << "): " << result
  //          << std::endl;
  io_result.post(boost::bind(&PrintResult, start, end, result, seq_num));
}

int main(void) {
  boost::thread_group threadpool;
  boost::thread_group threadpool_result;
  boost::asio::io_service::work* work = new boost::asio::io_service::work(io);
  boost::asio::io_service::work* work_result =
      new boost::asio::io_service::work(io_result);

  for (int i = 0; i < NUM_THREAD_IN_POOL; i++) {
    threadpool.create_thread(boost::bind(&boost::asio::io_service::run, &io));
  }

  for (int i = 0; i < NUM_RESULT_THREAD_IN_POOL; i++) {
    threadpool_result.create_thread(
        boost::bind(&boost::asio::io_service::run, &io_result));
  }

  int seq_num = 0;
  while (1) {
    int start, end;
    std::cin >> start;
    if (start < 0) {
      break;
    }
    std::cin >> end;

    io.post(boost::bind(&CntPrimeInRange, start, end, seq_num));
    seq_num++;
  }

  delete work;
  threadpool.join_all();
  io.stop();

  delete work_result;
  threadpool_result.join_all();
  io_result.stop();

  return 0;
}
