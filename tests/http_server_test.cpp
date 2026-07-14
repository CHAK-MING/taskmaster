#include "dagforge/http/http_server.hpp"
#include "dagforge/core/runtime.hpp"

#include "test_utils.hpp"
#include "gtest/gtest.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace dagforge::http::test {
namespace {

class ListeningSocket {
public:
  ListeningSocket() = default;
  ~ListeningSocket() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  ListeningSocket(const ListeningSocket &) = delete;
  auto operator=(const ListeningSocket &) -> ListeningSocket & = delete;

  auto bind_loopback(std::uint16_t port) -> bool {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
      return false;
    }

    int reuse = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
      return false;
    }
    return ::listen(fd_, SOMAXCONN) == 0;
  }

private:
  int fd_{-1};
};

} // namespace

TEST(HttpServerTest, StartFailsWhenPortAlreadyInUse) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  ListeningSocket occupied;
  ASSERT_TRUE(occupied.bind_loopback(port));

  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  HttpServer server(runtime);
  const auto result = server.start("127.0.0.1", port, false);

  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(server.is_running());

  server.stop();
  runtime.stop();
}

} // namespace dagforge::http::test
