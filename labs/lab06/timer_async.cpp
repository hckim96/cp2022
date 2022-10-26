#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <iostream>

void Print(const boost::system::error_code& e) {
  std::cout << "Hello, world!" << std::endl;
}

int main(void) {
  boost::asio::io_service io;

  boost::asio::deadline_timer t(io, boost::posix_time::seconds(5));

  // wait 5 seconds
  // Hello workld!
  // after async ~~
  // after io.run
  // t.wait(&Print);

  // after async~~~
  // 5 seconds of wait
  // Hello, world!
  // after io.run
  t.async_wait(&Print);
  std::cout << "after async_wait" << std::endl;

  io.run();  // Blocks the execution and executes anything within the io_service
  std::cout << "after io.run()" << std::endl;

  return 0;
}
