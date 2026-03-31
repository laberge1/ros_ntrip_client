#include "ros_ntrip_client/ntrip_client.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace
{

using ros_ntrip_client::NtripClient;
using ros_ntrip_client::NtripClientConfig;
using ros_ntrip_client::StatusCode;

constexpr std::chrono::milliseconds kShortDelay(50);
constexpr std::chrono::milliseconds kRequestReadTimeout(500);
const std::vector<std::uint8_t> kMinimalRtcmFrame = {0xD3, 0x00, 0x00, 0x47, 0xEA, 0x4B};

class MockCaster
{
public:
  using Handler = std::function<void(int client_fd)>;

  explicit MockCaster(Handler handler)
    : handler_(std::move(handler))
  {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0)
    {
      throw std::runtime_error("failed to create listen socket");
    }

    int enable = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) != 0)
    {
      throw std::runtime_error("failed to set SO_REUSEADDR");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(0);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
    {
      throw std::runtime_error("failed to bind listen socket");
    }
    if (listen(listen_fd_, 1) != 0)
    {
      throw std::runtime_error("failed to listen on mock caster");
    }

    socklen_t address_len = sizeof(address);
    if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address), &address_len) != 0)
    {
      throw std::runtime_error("failed to query mock caster port");
    }
    port_ = ntohs(address.sin_port);

    server_thread_ = std::thread([this]()
                                 {
                                   sockaddr_in client_address{};
                                   socklen_t client_len = sizeof(client_address);
                                   const int client_fd = accept(
                                       listen_fd_,
                                       reinterpret_cast<sockaddr*>(&client_address),
                                       &client_len);
                                   if (client_fd < 0)
                                   {
                                     return;
                                   }

                                   client_fd_ = client_fd;
                                   handler_(client_fd);
                                   shutdown(client_fd, SHUT_RDWR);
                                   close(client_fd);
                                   client_fd_ = -1;
                                 });
  }

  ~MockCaster()
  {
    stop();
  }

  int port() const
  {
    return port_;
  }

  void stop()
  {
    if (listen_fd_ >= 0)
    {
      shutdown(listen_fd_, SHUT_RDWR);
      close(listen_fd_);
      listen_fd_ = -1;
    }
    if (client_fd_ >= 0)
    {
      shutdown(client_fd_, SHUT_RDWR);
      close(client_fd_);
      client_fd_ = -1;
    }
    if (server_thread_.joinable())
    {
      server_thread_.join();
    }
  }

private:
  Handler handler_;
  int listen_fd_{-1};
  std::atomic<int> client_fd_{-1};
  int port_{-1};
  std::thread server_thread_;
};

class ScriptedCaster
{
public:
  using Handler = std::function<void(int client_fd)>;

  explicit ScriptedCaster(std::vector<Handler> handlers)
    : handlers_(std::move(handlers))
  {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0)
    {
      throw std::runtime_error("failed to create listen socket");
    }

    int enable = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) != 0)
    {
      throw std::runtime_error("failed to set SO_REUSEADDR");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(0);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
    {
      throw std::runtime_error("failed to bind listen socket");
    }
    if (listen(listen_fd_, 4) != 0)
    {
      throw std::runtime_error("failed to listen on scripted caster");
    }

    socklen_t address_len = sizeof(address);
    if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address), &address_len) != 0)
    {
      throw std::runtime_error("failed to query scripted caster port");
    }
    port_ = ntohs(address.sin_port);

    server_thread_ = std::thread([this]()
                                 {
                                   for (const Handler& handler : handlers_)
                                   {
                                     sockaddr_in client_address{};
                                     socklen_t client_len = sizeof(client_address);
                                     const int client_fd = accept(
                                         listen_fd_,
                                         reinterpret_cast<sockaddr*>(&client_address),
                                         &client_len);
                                     if (client_fd < 0)
                                     {
                                       return;
                                     }

                                     client_fd_ = client_fd;
                                     handler(client_fd);
                                     shutdown(client_fd, SHUT_RDWR);
                                     close(client_fd);
                                     client_fd_ = -1;
                                   }
                                 });
  }

  ~ScriptedCaster()
  {
    stop();
  }

  int port() const
  {
    return port_;
  }

  void stop()
  {
    if (listen_fd_ >= 0)
    {
      shutdown(listen_fd_, SHUT_RDWR);
      close(listen_fd_);
      listen_fd_ = -1;
    }
    if (client_fd_ >= 0)
    {
      shutdown(client_fd_, SHUT_RDWR);
      close(client_fd_);
      client_fd_ = -1;
    }
    if (server_thread_.joinable())
    {
      server_thread_.join();
    }
  }

private:
  std::vector<Handler> handlers_;
  int listen_fd_{-1};
  std::atomic<int> client_fd_{-1};
  int port_{-1};
  std::thread server_thread_;
};

std::string readRequest(int client_fd)
{
  timeval timeout{};
  timeout.tv_sec = 0;
  timeout.tv_usec = static_cast<suseconds_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(kRequestReadTimeout).count());
  setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  std::string request;
  std::array<char, 1024> buffer{};
  while (request.find("\r\n\r\n") == std::string::npos)
  {
    const ssize_t received = recv(client_fd, buffer.data(), buffer.size(), 0);
    if (received <= 0)
    {
      break;
    }
    request.append(buffer.data(), static_cast<std::size_t>(received));
  }
  return request;
}

bool waitForPredicate(
    const std::function<bool()>& predicate,
    std::chrono::milliseconds timeout = std::chrono::milliseconds(3000))
{
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline)
  {
    if (predicate())
    {
      return true;
    }
    std::this_thread::sleep_for(kShortDelay);
  }
  return predicate();
}

std::vector<std::string> copyStatuses(
    const std::vector<std::string>& statuses,
    std::mutex& mutex)
{
  std::lock_guard<std::mutex> lock(mutex);
  return statuses;
}

std::vector<StatusCode> copyStatusCodes(
    const std::vector<StatusCode>& codes,
    std::mutex& mutex)
{
  std::lock_guard<std::mutex> lock(mutex);
  return codes;
}

bool containsStatus(
    const std::vector<std::string>& statuses,
    const std::string& needle)
{
  for (const std::string& status : statuses)
  {
    if (status.find(needle) != std::string::npos)
    {
      return true;
    }
  }
  return false;
}

bool containsStatusCode(
    const std::vector<StatusCode>& codes,
    StatusCode needle)
{
  for (const StatusCode code : codes)
  {
    if (code == needle)
    {
      return true;
    }
  }
  return false;
}

std::size_t countStatuses(
    const std::vector<std::string>& statuses,
    const std::string& needle)
{
  std::size_t count = 0U;
  for (const std::string& status : statuses)
  {
    if (status.find(needle) != std::string::npos)
    {
      ++count;
    }
  }
  return count;
}

std::size_t countStatusCodes(
    const std::vector<StatusCode>& codes,
    StatusCode needle)
{
  std::size_t count = 0U;
  for (const StatusCode code : codes)
  {
    if (code == needle)
    {
      ++count;
    }
  }
  return count;
}

NtripClientConfig makeConfig(int port)
{
  NtripClientConfig config;
  config.host = "127.0.0.1";
  config.port = port;
  config.mountpoint = "TEST";
  config.read_timeout_sec = 0.1;
  config.connect_timeout_sec = 1.0;
  config.session_start_timeout_sec = 0.3;
  config.rtcm_timeout_sec = 0.3;
  config.reconnect_initial_delay_sec = 0.1;
  config.reconnect_max_delay_sec = 0.1;
  config.transport_reconnect_initial_delay_sec = 0.1;
  config.transport_reconnect_max_delay_sec = 0.1;
  config.max_attempts = 1;
  config.adaptive_reconnect = false;
  return config;
}

}  // namespace

TEST(NtripClientTest, PublishesRtcmAndTransitionsToStreamActive)
{
  MockCaster caster([](int client_fd)
                    {
                      const std::string request = readRequest(client_fd);
                      EXPECT_NE(request.find("GET /TEST HTTP/1.1"), std::string::npos);

                      const std::string response = "ICY 200 OK\r\n";
                      ASSERT_EQ(send(client_fd, response.data(), response.size(), 0),
                                static_cast<ssize_t>(response.size()));
                      ASSERT_EQ(send(client_fd,
                                     kMinimalRtcmFrame.data(),
                                     kMinimalRtcmFrame.size(),
                                     0),
                                static_cast<ssize_t>(kMinimalRtcmFrame.size()));
                      std::this_thread::sleep_for(std::chrono::milliseconds(200));
                    });

  NtripClientConfig config = makeConfig(caster.port());
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;
  std::mutex code_mutex;
  std::vector<StatusCode> codes;
  std::mutex data_mutex;
  std::vector<std::vector<std::uint8_t>> frames;

  ASSERT_TRUE(client.start(
      [&](const std::vector<std::uint8_t>& frame)
      {
        std::lock_guard<std::mutex> lock(data_mutex);
        frames.push_back(frame);
      },
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
        std::lock_guard<std::mutex> code_lock(code_mutex);
        codes.push_back(status.code);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 std::lock_guard<std::mutex> lock(data_mutex);
                                 return !frames.empty();
                               }));

  client.stop();
  caster.stop();

  {
    std::lock_guard<std::mutex> lock(data_mutex);
    ASSERT_EQ(frames.size(), 1U);
    EXPECT_EQ(frames.front(), kMinimalRtcmFrame);
  }

  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::SessionAccepted));
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::StreamActive));
  EXPECT_TRUE(containsStatus(copied_statuses, "caster accepted stream"));
  EXPECT_TRUE(containsStatus(copied_statuses, "RTCM stream active"));

  const ros_ntrip_client::NtripClientCounters counters = client.getCounters();
  EXPECT_GE(counters.bytes_received, kMinimalRtcmFrame.size());
  EXPECT_EQ(counters.frames_published, 1U);
}

TEST(NtripClientTest, TimesOutWaitingForHeaders)
{
  MockCaster caster([](int client_fd)
                    {
                      (void)readRequest(client_fd);
                      std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    });

  NtripClientConfig config = makeConfig(caster.port());
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;
  std::mutex code_mutex;
  std::vector<StatusCode> codes;

  ASSERT_TRUE(client.start(
      [](const std::vector<std::uint8_t>&) {},
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
        std::lock_guard<std::mutex> code_lock(code_mutex);
        codes.push_back(status.code);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 const std::vector<std::string> copied = copyStatuses(statuses, status_mutex);
                                 return containsStatus(copied, "timed out waiting for caster response headers");
                               }));

  client.stop();
  caster.stop();

  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::TransportHeaderFailed));
  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  EXPECT_TRUE(containsStatus(copied_statuses, "timed out waiting for caster response headers"));
}

TEST(NtripClientTest, TimesOutWhenSessionDoesNotProduceFirstRtcmFrame)
{
  MockCaster caster([](int client_fd)
                    {
                      (void)readRequest(client_fd);
                      const std::string response = "ICY 200 OK\r\n";
                      ASSERT_EQ(send(client_fd, response.data(), response.size(), 0),
                                static_cast<ssize_t>(response.size()));
                      std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    });

  NtripClientConfig config = makeConfig(caster.port());
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;
  std::mutex code_mutex;
  std::vector<StatusCode> codes;

  ASSERT_TRUE(client.start(
      [](const std::vector<std::uint8_t>&) {},
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
        std::lock_guard<std::mutex> code_lock(code_mutex);
        codes.push_back(status.code);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 const std::vector<std::string> copied = copyStatuses(statuses, status_mutex);
                                 return containsStatus(copied, "RTCM stream did not start within");
                               }));

  client.stop();
  caster.stop();

  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::SessionAccepted));
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::SessionStartTimeout));
  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  EXPECT_TRUE(containsStatus(copied_statuses, "caster accepted stream"));
  EXPECT_TRUE(containsStatus(copied_statuses, "RTCM stream did not start within"));
}

TEST(NtripClientTest, ReportsAcceptedButEmptySession)
{
  MockCaster caster([](int client_fd)
                    {
                      (void)readRequest(client_fd);
                      const std::string response = "ICY 200 OK\r\n";
                      ASSERT_EQ(send(client_fd, response.data(), response.size(), 0),
                                static_cast<ssize_t>(response.size()));
                      std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    });

  NtripClientConfig config = makeConfig(caster.port());
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;
  std::mutex code_mutex;
  std::vector<StatusCode> codes;

  ASSERT_TRUE(client.start(
      [](const std::vector<std::uint8_t>&) {},
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
        std::lock_guard<std::mutex> code_lock(code_mutex);
        codes.push_back(status.code);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 const std::vector<std::string> copied = copyStatuses(statuses, status_mutex);
                                 return containsStatus(
                                     copied,
                                     "caster accepted session but closed before sending any stream data");
                               }));

  client.stop();
  caster.stop();

  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::SessionEmpty));
  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  EXPECT_TRUE(containsStatus(
      copied_statuses,
      "caster accepted session but closed before sending any stream data"));
}

TEST(NtripClientTest, TimesOutAfterActiveStreamStopsDeliveringRtcm)
{
  MockCaster caster([](int client_fd)
                    {
                      (void)readRequest(client_fd);
                      const std::string response = "ICY 200 OK\r\n";
                      ASSERT_EQ(send(client_fd, response.data(), response.size(), 0),
                                static_cast<ssize_t>(response.size()));
                      ASSERT_EQ(send(client_fd,
                                     kMinimalRtcmFrame.data(),
                                     kMinimalRtcmFrame.size(),
                                     0),
                                static_cast<ssize_t>(kMinimalRtcmFrame.size()));
                      std::this_thread::sleep_for(std::chrono::milliseconds(700));
                    });

  NtripClientConfig config = makeConfig(caster.port());
  config.max_attempts = 1;
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;
  std::mutex code_mutex;
  std::vector<StatusCode> codes;

  ASSERT_TRUE(client.start(
      [](const std::vector<std::uint8_t>&) {},
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
        std::lock_guard<std::mutex> code_lock(code_mutex);
        codes.push_back(status.code);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 const std::vector<std::string> copied = copyStatuses(statuses, status_mutex);
                                 return containsStatus(copied, "RTCM data not received for");
                               }));

  client.stop();
  caster.stop();

  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::StreamActive));
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::RtcmTimeout));
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::Backoff));
  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  EXPECT_TRUE(containsStatus(copied_statuses, "RTCM stream active"));
  EXPECT_TRUE(containsStatus(copied_statuses, "RTCM data not received for"));
  EXPECT_TRUE(containsStatus(copied_statuses, "stream disconnected, backing off for"));
}

TEST(NtripClientTest, ReportsNoValidRtcmWhenStreamContainsNonRtcmBytes)
{
  MockCaster caster([](int client_fd)
                    {
                      (void)readRequest(client_fd);
                      const std::string response = "ICY 200 OK\r\n";
                      const std::string payload = "NOT_RTCM_STREAM_DATA";
                      ASSERT_EQ(send(client_fd, response.data(), response.size(), 0),
                                static_cast<ssize_t>(response.size()));
                      ASSERT_EQ(send(client_fd, payload.data(), payload.size(), 0),
                                static_cast<ssize_t>(payload.size()));
                      std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    });

  NtripClientConfig config = makeConfig(caster.port());
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;
  std::mutex code_mutex;
  std::vector<StatusCode> codes;

  ASSERT_TRUE(client.start(
      [](const std::vector<std::uint8_t>&) {},
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
        std::lock_guard<std::mutex> code_lock(code_mutex);
        codes.push_back(status.code);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 const std::vector<std::string> copied = copyStatuses(statuses, status_mutex);
                                 return containsStatus(
                                     copied,
                                     "caster accepted session but no valid RTCM frames were received before close");
                               }));

  client.stop();
  caster.stop();

  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::SessionNoValidRtcm));
  const ros_ntrip_client::NtripClientCounters counters = client.getCounters();
  EXPECT_GT(counters.bytes_received, 0U);
  EXPECT_EQ(counters.frames_published, 0U);
  EXPECT_EQ(counters.crc_failures, 0U);
  EXPECT_GT(counters.discarded_bytes, 0U);
}

TEST(NtripClientTest, ResetsFailureStateAfterHealthyStreamAndUsesTransportBackoff)
{
  ScriptedCaster caster({
      [](int client_fd)
      {
        (void)readRequest(client_fd);
        const std::string response = "ICY 200 OK\r\n";
        ASSERT_EQ(send(client_fd, response.data(), response.size(), 0),
                  static_cast<ssize_t>(response.size()));
        ASSERT_EQ(send(client_fd,
                       kMinimalRtcmFrame.data(),
                       kMinimalRtcmFrame.size(),
                       0),
                  static_cast<ssize_t>(kMinimalRtcmFrame.size()));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      },
      [](int client_fd)
      {
        (void)readRequest(client_fd);
        const std::string response = "ICY 200 OK\r\n";
        ASSERT_EQ(send(client_fd, response.data(), response.size(), 0),
                  static_cast<ssize_t>(response.size()));
        ASSERT_EQ(send(client_fd,
                       kMinimalRtcmFrame.data(),
                       kMinimalRtcmFrame.size(),
                       0),
                  static_cast<ssize_t>(kMinimalRtcmFrame.size()));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }});

  NtripClientConfig config = makeConfig(caster.port());
  config.max_attempts = 3;
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;
  std::mutex code_mutex;
  std::vector<StatusCode> codes;

  ASSERT_TRUE(client.start(
      [](const std::vector<std::uint8_t>&) {},
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
        std::lock_guard<std::mutex> code_lock(code_mutex);
        codes.push_back(status.code);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 const std::vector<std::string> copied = copyStatuses(statuses, status_mutex);
                                 return countStatuses(copied, "RTCM stream active") >= 2U;
                               }));

  client.stop();
  caster.stop();

  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_GE(countStatusCodes(copied_codes, StatusCode::StreamRecovered), 2U);
  EXPECT_GE(countStatusCodes(copied_codes, StatusCode::StreamActive), 2U);
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::Backoff));
  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  EXPECT_GE(countStatuses(copied_statuses, "successful stream established; reconnect state reset"), 2U);
  EXPECT_TRUE(containsStatus(copied_statuses, "transport stream disconnected, backing off for"));
}

TEST(NtripClientTest, MaxAttemptsAppliesToRepeatedTransportFailures)
{
  const int failing_port = 65001;
  NtripClientConfig config = makeConfig(failing_port);
  config.max_attempts = 2;
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;
  std::mutex code_mutex;
  std::vector<StatusCode> codes;

  ASSERT_TRUE(client.start(
      [](const std::vector<std::uint8_t>&) {},
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
        std::lock_guard<std::mutex> code_lock(code_mutex);
        codes.push_back(status.code);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 const std::vector<std::string> copied = copyStatuses(statuses, status_mutex);
                                 return containsStatus(copied, "maximum connection attempts reached");
                               },
                               std::chrono::milliseconds(5000)));

  client.stop();

  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_EQ(countStatusCodes(copied_codes, StatusCode::Connecting), 2U);
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::StoppedMaxAttempts));
  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  EXPECT_EQ(countStatuses(copied_statuses, "connection attempt "), 2U);
  EXPECT_TRUE(containsStatus(copied_statuses, "maximum connection attempts reached"));
}
