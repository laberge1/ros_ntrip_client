#include "ros_ntrip_client/ntrip_client.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <cstdlib>
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

class TempFile
{
public:
  explicit TempFile(const std::string& contents)
  {
    char path_template[] = "/tmp/ros_ntrip_client_test_XXXXXX";
    const int fd = mkstemp(path_template);
    if (fd < 0)
    {
      throw std::runtime_error("failed to create temporary file");
    }

    path_ = path_template;
    const ssize_t written = write(fd, contents.data(), contents.size());
    close(fd);
    if (written != static_cast<ssize_t>(contents.size()))
    {
      std::remove(path_.c_str());
      throw std::runtime_error("failed to write temporary file contents");
    }
  }

  ~TempFile()
  {
    if (!path_.empty())
    {
      std::remove(path_.c_str());
    }
  }

  const std::string& path() const
  {
    return path_;
  }

private:
  std::string path_;
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

double extractBackoffSeconds(const std::string& status)
{
  const std::size_t marker = status.rfind(" for ");
  if (marker == std::string::npos)
  {
    return -1.0;
  }
  const std::size_t seconds_suffix = status.find('s', marker + 5U);
  if (seconds_suffix == std::string::npos)
  {
    return -1.0;
  }

  const std::string number = status.substr(marker + 5U, seconds_suffix - (marker + 5U));
  return std::strtod(number.c_str(), nullptr);
}

double latestBackoffSeconds(const std::vector<std::string>& statuses)
{
  for (auto it = statuses.rbegin(); it != statuses.rend(); ++it)
  {
    if (it->find("backing off for ") != std::string::npos)
    {
      return extractBackoffSeconds(*it);
    }
  }
  return -1.0;
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

void expectResponseClassification(
    const std::string& response,
    StatusCode expected_code,
    const std::string& expected_message_snippet)
{
  MockCaster caster([&](int client_fd)
                    {
                      (void)readRequest(client_fd);
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
                                 const std::vector<std::string> copied =
                                     copyStatuses(statuses, status_mutex);
                                 return containsStatus(copied, expected_message_snippet);
                               }));

  client.stop();
  caster.stop();

  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_TRUE(containsStatusCode(copied_codes, expected_code));
  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  EXPECT_TRUE(containsStatus(copied_statuses, expected_message_snippet));
}

const char kTestCertificatePem[] =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIBqTCCAU+gAwIBAgIUe3VTNff7kwh28ykVfoCENKz7LQ0wCgYIKoZIzj0EAwIw\n"
    "GDEWMBQGA1UEAwwNcm9zLW50cmlwLXRlc3QwHhcNMjYwMzMxMDQwMDAwWhcNMzYw\n"
    "MzI5MDQwMDAwWjAYMRYwFAYDVQQDDA1yb3MtbnRyaXAtdGVzdDBZMBMGByqGSM49\n"
    "AgEGCCqGSM49AwEHA0IABJ6o6hMHDL/95B2S/bRMyCV2wAPOQgpdnXl16rDpD+s/\n"
    "xkD114F8CbnMD4HzyBbs6k8ZZrVSu2Ce279b9Ec/WWijUzBRMB0GA1UdDgQWBBRz\n"
    "y83H8XTur2qxGn8pY/+bexdFvDAfBgNVHSMEGDAWgBRzy83H8XTur2qxGn8pY/+b\n"
    "exdFvDAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0gAMEUCIDdxHDBPPfQj\n"
    "TA70vsK1tnE+bBZ5qTqL0U8nyCLlcQFV AiEAxgN9zeps2sonMSKcwk5Y8ZndKyV+\n"
    "XS6/4FwXk4Hknc0=\n"
    "-----END CERTIFICATE-----\n";

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

TEST(NtripClientTest, ClassifiesSourcetableResponseAsInvalidMountpoint)
{
  expectResponseClassification(
      "SOURCETABLE 200 OK\r\nContent-Type: text/plain\r\n\r\n",
      StatusCode::MountpointInvalid,
      "received sourcetable response; mountpoint is likely invalid");
}

TEST(NtripClientTest, ClassifiesUnauthorizedResponse)
{
  expectResponseClassification(
      "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n",
      StatusCode::AuthFailed,
      "received unauthorized response; check username, password, and mountpoint");
}

TEST(NtripClientTest, ClassifiesNotFoundResponse)
{
  expectResponseClassification(
      "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
      StatusCode::MountpointInvalid,
      "received not-found response; mountpoint or path is likely invalid");
}

TEST(NtripClientTest, ClassifiesTooManyRequestsResponse)
{
  expectResponseClassification(
      "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 60\r\n\r\n",
      StatusCode::RateLimited,
      "received too-many-requests response; caster is rate limiting this client");
}

TEST(NtripClientTest, ClassifiesServiceUnavailableResponse)
{
  expectResponseClassification(
      "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
      StatusCode::ServiceUnavailable,
      "received service-unavailable response; caster is temporarily unavailable");
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

TEST(NtripClientTest, TransportConnectFailureUsesTransportBackoffSettings)
{
  const int failing_port = 65002;
  NtripClientConfig config = makeConfig(failing_port);
  config.max_attempts = 1;
  config.transport_reconnect_initial_delay_sec = 0.8;
  config.transport_reconnect_max_delay_sec = 0.8;
  config.reconnect_initial_delay_sec = 5.0;
  config.reconnect_max_delay_sec = 5.0;
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;

  ASSERT_TRUE(client.start(
      [](const std::vector<std::uint8_t>&) {},
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 const std::vector<std::string> copied =
                                     copyStatuses(statuses, status_mutex);
                                 return containsStatus(copied, "transport connect failed, backing off for");
                               }));

  client.stop();

  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  const double backoff_seconds = latestBackoffSeconds(copied_statuses);
  EXPECT_GE(backoff_seconds, 0.70);
  EXPECT_LE(backoff_seconds, 0.90);
}

TEST(NtripClientTest, ServiceFailureUsesServiceBackoffSettings)
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
  config.max_attempts = 1;
  config.session_start_timeout_sec = 0.2;
  config.reconnect_initial_delay_sec = 0.8;
  config.reconnect_max_delay_sec = 0.8;
  config.transport_reconnect_initial_delay_sec = 5.0;
  config.transport_reconnect_max_delay_sec = 5.0;
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;

  ASSERT_TRUE(client.start(
      [](const std::vector<std::uint8_t>&) {},
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 const std::vector<std::string> copied =
                                     copyStatuses(statuses, status_mutex);
                                 return containsStatus(copied, "stream disconnected, backing off for");
                               }));

  client.stop();
  caster.stop();

  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  const double backoff_seconds = latestBackoffSeconds(copied_statuses);
  EXPECT_GE(backoff_seconds, 0.70);
  EXPECT_LE(backoff_seconds, 0.90);
}

TEST(NtripClientTest, AdaptiveBurstMinimumDelayOverridesServiceBackoff)
{
  MockCaster caster([](int client_fd)
                    {
                      (void)readRequest(client_fd);
                      const std::string response = "ICY 200 OK\r\n";
                      ASSERT_EQ(send(client_fd, response.data(), response.size(), 0),
                                static_cast<ssize_t>(response.size()));
                      std::this_thread::sleep_for(std::chrono::milliseconds(200));
                    });

  NtripClientConfig config = makeConfig(caster.port());
  config.max_attempts = 1;
  config.session_start_timeout_sec = 0.05;
  config.adaptive_reconnect = true;
  config.adaptive_burst_max_attempts = 1;
  config.adaptive_burst_window_sec = 0.8;
  config.reconnect_initial_delay_sec = 0.1;
  config.reconnect_max_delay_sec = 0.1;
  NtripClient client(config);

  std::mutex status_mutex;
  std::vector<std::string> statuses;

  ASSERT_TRUE(client.start(
      [](const std::vector<std::uint8_t>&) {},
      [&](const ros_ntrip_client::StatusEvent& status)
      {
        std::lock_guard<std::mutex> lock(status_mutex);
        statuses.push_back(status.message);
      }));

  ASSERT_TRUE(waitForPredicate([&]()
                               {
                                 const std::vector<std::string> copied =
                                     copyStatuses(statuses, status_mutex);
                                 return containsStatus(copied, "stream disconnected, backing off for");
                               }));

  client.stop();
  caster.stop();

  const std::vector<std::string> copied_statuses = copyStatuses(statuses, status_mutex);
  const double backoff_seconds = latestBackoffSeconds(copied_statuses);
  EXPECT_GE(backoff_seconds, 0.70);
}

TEST(NtripClientTest, ReportsTlsHandshakeFailureAgainstPlainTcpCaster)
{
  MockCaster caster([](int client_fd)
                    {
                      (void)readRequest(client_fd);
                      std::this_thread::sleep_for(std::chrono::milliseconds(200));
                    });

  NtripClientConfig config = makeConfig(caster.port());
  config.tls_enabled = true;
  config.tls_verify_peer = false;
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
                                 const std::vector<std::string> copied =
                                     copyStatuses(statuses, status_mutex);
                                 return containsStatus(copied, "TLS handshake failed:");
                               }));

  client.stop();
  caster.stop();

  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::TlsError));
}

TEST(NtripClientTest, RejectsPartialMtlsConfiguration)
{
  TempFile certificate(kTestCertificatePem);
  MockCaster caster([](int client_fd)
                    {
                      std::this_thread::sleep_for(std::chrono::milliseconds(200));
                      shutdown(client_fd, SHUT_RDWR);
                    });

  NtripClientConfig config = makeConfig(caster.port());
  config.tls_enabled = true;
  config.tls_verify_peer = false;
  config.tls_client_cert_file = certificate.path();
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
                                 const std::vector<std::string> copied =
                                     copyStatuses(statuses, status_mutex);
                                 return containsStatus(
                                     copied,
                                     "both tls_client_cert_file and tls_client_key_file are required for mTLS");
                               }));

  client.stop();
  caster.stop();

  const std::vector<StatusCode> copied_codes = copyStatusCodes(codes, code_mutex);
  EXPECT_TRUE(containsStatusCode(copied_codes, StatusCode::TlsError));
}
