#include "ros_ntrip_client/ntrip_client.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/ssl.h>

namespace ros_ntrip_client
{
namespace
{

std::once_flag g_openssl_init_once;
constexpr std::uint8_t kRtcmPreamble = 0xD3U;
constexpr std::size_t kMaxRtcmBufferSize = 10240U;
constexpr ssize_t kReadTimeoutResult = -2;
constexpr std::uint32_t kRtcmCrcLookup[] = {
    0x000000, 0x864CFB, 0x8AD50D, 0x0C99F6, 0x93E6E1, 0x15AA1A, 0x1933EC, 0x9F7F17,
    0xA18139, 0x27CDC2, 0x2B5434, 0xAD18CF, 0x3267D8, 0xB42B23, 0xB8B2D5, 0x3EFE2E,
    0xC54E89, 0x430272, 0x4F9B84, 0xC9D77F, 0x56A868, 0xD0E493, 0xDC7D65, 0x5A319E,
    0x64CFB0, 0xE2834B, 0xEE1ABD, 0x685646, 0xF72951, 0x7165AA, 0x7DFC5C, 0xFBB0A7,
    0x0CD1E9, 0x8A9D12, 0x8604E4, 0x00481F, 0x9F3708, 0x197BF3, 0x15E205, 0x93AEFE,
    0xAD50D0, 0x2B1C2B, 0x2785DD, 0xA1C926, 0x3EB631, 0xB8FACA, 0xB4633C, 0x322FC7,
    0xC99F60, 0x4FD39B, 0x434A6D, 0xC50696, 0x5A7981, 0xDC357A, 0xD0AC8C, 0x56E077,
    0x681E59, 0xEE52A2, 0xE2CB54, 0x6487AF, 0xFBF8B8, 0x7DB443, 0x712DB5, 0xF7614E,
    0x19A3D2, 0x9FEF29, 0x9376DF, 0x153A24, 0x8A4533, 0x0C09C8, 0x00903E, 0x86DCC5,
    0xB822EB, 0x3E6E10, 0x32F7E6, 0xB4BB1D, 0x2BC40A, 0xAD88F1, 0xA11107, 0x275DFC,
    0xDCED5B, 0x5AA1A0, 0x563856, 0xD074AD, 0x4F0BBA, 0xC94741, 0xC5DEB7, 0x43924C,
    0x7D6C62, 0xFB2099, 0xF7B96F, 0x71F594, 0xEE8A83, 0x68C678, 0x645F8E, 0xE21375,
    0x15723B, 0x933EC0, 0x9FA736, 0x19EBCD, 0x8694DA, 0x00D821, 0x0C41D7, 0x8A0D2C,
    0xB4F302, 0x32BFF9, 0x3E260F, 0xB86AF4, 0x2715E3, 0xA15918, 0xADC0EE, 0x2B8C15,
    0xD03CB2, 0x567049, 0x5AE9BF, 0xDCA544, 0x43DA53, 0xC596A8, 0xC90F5E, 0x4F43A5,
    0x71BD8B, 0xF7F170, 0xFB6886, 0x7D247D, 0xE25B6A, 0x641791, 0x688E67, 0xEEC29C,
    0x3347A4, 0xB50B5F, 0xB992A9, 0x3FDE52, 0xA0A145, 0x26EDBE, 0x2A7448, 0xAC38B3,
    0x92C69D, 0x148A66, 0x181390, 0x9E5F6B, 0x01207C, 0x876C87, 0x8BF571, 0x0DB98A,
    0xF6092D, 0x7045D6, 0x7CDC20, 0xFA90DB, 0x65EFCC, 0xE3A337, 0xEF3AC1, 0x69763A,
    0x578814, 0xD1C4EF, 0xDD5D19, 0x5B11E2, 0xC46EF5, 0x42220E, 0x4EBBF8, 0xC8F703,
    0x3F964D, 0xB9DAB6, 0xB54340, 0x330FBB, 0xAC70AC, 0x2A3C57, 0x26A5A1, 0xA0E95A,
    0x9E1774, 0x185B8F, 0x14C279, 0x928E82, 0x0DF195, 0x8BBD6E, 0x872498, 0x016863,
    0xFAD8C4, 0x7C943F, 0x700DC9, 0xF64132, 0x693E25, 0xEF72DE, 0xE3EB28, 0x65A7D3,
    0x5B59FD, 0xDD1506, 0xD18CF0, 0x57C00B, 0xC8BF1C, 0x4EF3E7, 0x426A11, 0xC426EA,
    0x2AE476, 0xACA88D, 0xA0317B, 0x267D80, 0xB90297, 0x3F4E6C, 0x33D79A, 0xB59B61,
    0x8B654F, 0x0D29B4, 0x01B042, 0x87FCB9, 0x1883AE, 0x9ECF55, 0x9256A3, 0x141A58,
    0xEFAAFF, 0x69E604, 0x657FF2, 0xE33309, 0x7C4C1E, 0xFA00E5, 0xF69913, 0x70D5E8,
    0x4E2BC6, 0xC8673D, 0xC4FECB, 0x42B230, 0xDDCD27, 0x5B81DC, 0x57182A, 0xD154D1,
    0x26359F, 0xA07964, 0xACE092, 0x2AAC69, 0xB5D37E, 0x339F85, 0x3F0673, 0xB94A88,
    0x87B4A6, 0x01F85D, 0x0D61AB, 0x8B2D50, 0x145247, 0x921EBC, 0x9E874A, 0x18CBB1,
    0xE37B16, 0x6537ED, 0x69AE1B, 0xEFE2E0, 0x709DF7, 0xF6D10C, 0xFA48FA, 0x7C0401,
    0x42FA2F, 0xC4B6D4, 0xC82F22, 0x4E63D9, 0xD11CCE, 0x575035, 0x5BC9C3, 0xDD8538};

std::string base64Encode(const std::string& input)
{
  static const char alphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  std::string output;
  output.reserve(((input.size() + 2U) / 3U) * 4U);

  std::uint32_t accumulator = 0U;
  int bits_collected = 0;
  for (unsigned char ch : input)
  {
    accumulator = (accumulator << 8U) | ch;
    bits_collected += 8;
    while (bits_collected >= 6)
    {
      bits_collected -= 6;
      output.push_back(alphabet[(accumulator >> bits_collected) & 0x3FU]);
    }
  }

  if (bits_collected > 0)
  {
    accumulator <<= (6 - bits_collected);
    output.push_back(alphabet[accumulator & 0x3FU]);
  }

  while ((output.size() % 4U) != 0U)
  {
    output.push_back('=');
  }

  return output;
}

timeval toTimeval(double seconds)
{
  const double bounded = std::max(0.0, seconds);
  timeval tv;
  tv.tv_sec = static_cast<long>(bounded);
  tv.tv_usec = static_cast<long>((bounded - static_cast<double>(tv.tv_sec)) * 1e6);
  return tv;
}

bool waitForSocket(int socket_fd, bool write_ready, double timeout_sec)
{
  fd_set fds;
  FD_ZERO(&fds);
  FD_SET(socket_fd, &fds);
  timeval timeout = toTimeval(timeout_sec);
  const int rc = select(socket_fd + 1,
                        write_ready ? nullptr : &fds,
                        write_ready ? &fds : nullptr,
                        nullptr,
                        &timeout);
  return rc > 0;
}

std::string trimMountpoint(const std::string& mountpoint)
{
  if (mountpoint.empty())
  {
    return "/";
  }
  if (mountpoint.front() == '/')
  {
    return mountpoint;
  }
  return "/" + mountpoint;
}

std::size_t findHeaderEnd(const std::string& response)
{
  if (response.rfind("ICY 200 OK\r\n", 0) == 0)
  {
    return std::string("ICY 200 OK\r\n").size();
  }

  if (response.rfind("ICY 200 OK\n", 0) == 0)
  {
    return std::string("ICY 200 OK\n").size();
  }

  const std::size_t crlf_end = response.find("\r\n\r\n");
  if (crlf_end != std::string::npos)
  {
    return crlf_end + 4U;
  }

  const std::size_t lf_end = response.find("\n\n");
  if (lf_end != std::string::npos)
  {
    return lf_end + 2U;
  }

  return std::string::npos;
}

std::string sanitizeSnippet(const std::string& input, std::size_t max_length)
{
  std::string snippet = input.substr(0, std::min(input.size(), max_length));
  for (char& ch : snippet)
  {
    if (ch == '\r' || ch == '\n')
    {
      ch = ' ';
    }
  }
  return snippet;
}

bool containsAny(const std::string& haystack, const std::initializer_list<const char*> needles)
{
  for (const char* needle : needles)
  {
    if (haystack.find(needle) != std::string::npos)
    {
      return true;
    }
  }
  return false;
}

std::string currentSslError()
{
  const unsigned long error_code = ERR_get_error();
  if (error_code == 0UL)
  {
    return "unknown TLS error";
  }

  std::array<char, 256> error_buffer{};
  ERR_error_string_n(error_code, error_buffer.data(), error_buffer.size());
  return std::string(error_buffer.data());
}

int sslPasswordCallback(char* buffer, int size, int, void* userdata)
{
  if (buffer == nullptr || userdata == nullptr || size <= 0)
  {
    return 0;
  }

  const std::string* password = static_cast<const std::string*>(userdata);
  const int password_length = static_cast<int>(std::min<std::size_t>(
      password->size(), static_cast<std::size_t>(size - 1)));
  std::memcpy(buffer, password->data(), static_cast<std::size_t>(password_length));
  buffer[password_length] = '\0';
  return password_length;
}

}  // namespace

NtripClient::NtripClient(NtripClientConfig config)
  : config_(std::move(config))
{
  rtcm_buffer_.reserve(kMaxRtcmBufferSize);
}

NtripClient::~NtripClient()
{
  stop();
}

bool NtripClient::start(DataCallback data_callback, StatusCallback status_callback)
{
  if (!data_callback)
  {
    setStatus("refusing to start without a data callback");
    return false;
  }

  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true))
  {
    setStatus("client is already running");
    return false;
  }

  data_callback_ = std::move(data_callback);
  status_callback_ = std::move(status_callback);
  worker_thread_ = std::thread(&NtripClient::workerLoop, this);
  return true;
}

void NtripClient::stop()
{
  if (!running_.exchange(false))
  {
    return;
  }

  int socket_to_close = -1;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    socket_to_close = active_socket_;
  }

  if (socket_to_close >= 0)
  {
    shutdown(socket_to_close, SHUT_RDWR);
    close(socket_to_close);
  }

  if (worker_thread_.joinable())
  {
    worker_thread_.join();
  }
}

void NtripClient::updateGgaSentence(const std::string& gga_sentence)
{
  std::lock_guard<std::mutex> lock(mutex_);
  latest_gga_sentence_ = gga_sentence;
  if (active_socket_ >= 0)
  {
    sendLatestGgaLocked(active_socket_);
  }
}

void NtripClient::workerLoop()
{
  int attempts = 0;

  while (running_)
  {
    if (config_.max_attempts > 0 && attempts >= config_.max_attempts)
    {
      setStatus("maximum connection attempts reached");
      break;
    }

    ++attempts;
    std::ostringstream attempt_msg;
    attempt_msg << "connection attempt " << attempts;
    setStatus(attempt_msg.str());

    int socket_fd = connectToCaster();
    if (socket_fd < 0)
    {
      recordFailureAttempt();
      const double delay_sec = std::max(computeBackoffDelaySec(attempts),
                                        computeAdaptiveMinimumDelaySec());
      std::ostringstream msg;
      msg << "connect failed, backing off for " << std::fixed << std::setprecision(2)
          << delay_sec << "s";
      setStatus(msg.str());
      if (!sleepForSeconds(delay_sec))
      {
        break;
      }
      continue;
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      active_socket_ = socket_fd;
    }
    rtcm_buffer_.clear();

    const bool request_sent = sendRequest(socket_fd);
    std::string headers;
    const bool response_ok = request_sent && readResponseHeaders(socket_fd, headers);
    if (response_ok)
    {
      attempts = 0;
      resetFailureTracking();
    }
    const bool streamed_ok = response_ok && streamData(socket_fd);

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (active_socket_ == socket_fd)
      {
        cleanupActiveTransportLocked();
      }
    }

    if (!running_)
    {
      break;
    }

    if (!streamed_ok)
    {
      recordFailureAttempt();
      const double delay_sec = std::max(computeBackoffDelaySec(attempts),
                                        computeAdaptiveMinimumDelaySec());
      std::ostringstream msg;
      msg << "stream disconnected, backing off for " << std::fixed << std::setprecision(2)
          << delay_sec << "s";
      setStatus(msg.str());
      if (!sleepForSeconds(delay_sec))
      {
        break;
      }
      continue;
    }
  }

  running_ = false;
}

int NtripClient::connectToCaster()
{
  std::call_once(g_openssl_init_once, []()
                 {
                   SSL_library_init();
                   SSL_load_error_strings();
                   OPENSSL_init_ssl(0, nullptr);
                 });

  addrinfo hints;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo* addresses = nullptr;
  const std::string port_str = std::to_string(config_.port);
  const int rc = getaddrinfo(config_.host.c_str(), port_str.c_str(), &hints, &addresses);
  if (rc != 0)
  {
    setStatus(std::string("DNS resolution failed: ") + gai_strerror(rc));
    return -1;
  }

  int connected_socket = -1;
  for (addrinfo* addr = addresses; addr != nullptr && running_; addr = addr->ai_next)
  {
    const int socket_fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
    if (socket_fd < 0)
    {
      continue;
    }

    const int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags < 0 || fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
      close(socket_fd);
      continue;
    }

    int connect_rc = connect(socket_fd, addr->ai_addr, addr->ai_addrlen);
    if (connect_rc < 0 && errno == EINPROGRESS)
    {
      if (!waitForSocket(socket_fd, true, config_.connect_timeout_sec))
      {
        close(socket_fd);
        continue;
      }

      int socket_error = 0;
      socklen_t error_len = sizeof(socket_error);
      if (getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_len) < 0 ||
          socket_error != 0)
      {
        close(socket_fd);
        continue;
      }
    }
    else if (connect_rc < 0)
    {
      close(socket_fd);
      continue;
    }

    if (fcntl(socket_fd, F_SETFL, flags) < 0)
    {
      close(socket_fd);
      continue;
    }

    const timeval timeout = toTimeval(config_.read_timeout_sec);
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    const int enable = 1;
    setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(enable));

    if (config_.tls_enabled)
    {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        active_socket_ = socket_fd;
      }

      if (!configureTlsForSocket(socket_fd))
      {
        std::lock_guard<std::mutex> lock(mutex_);
        cleanupActiveTransportLocked();
        continue;
      }
    }

    connected_socket = socket_fd;
    break;
  }

  freeaddrinfo(addresses);

  if (connected_socket >= 0)
  {
    setStatus("connected to caster");
  }
  else
  {
    setStatus("unable to connect to any resolved address");
  }
  return connected_socket;
}

bool NtripClient::configureTlsForSocket(int socket_fd)
{
  SSL_CTX* ssl_ctx = SSL_CTX_new(TLS_client_method());
  if (ssl_ctx == nullptr)
  {
    setStatus(std::string("failed to create TLS context: ") + currentSslError());
    return false;
  }

  if (config_.tls_verify_peer)
  {
    SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, nullptr);
    if (!config_.tls_ca_cert_file.empty() || !config_.tls_ca_cert_path.empty())
    {
      const int load_ok = SSL_CTX_load_verify_locations(
          ssl_ctx,
          config_.tls_ca_cert_file.empty() ? nullptr : config_.tls_ca_cert_file.c_str(),
          config_.tls_ca_cert_path.empty() ? nullptr : config_.tls_ca_cert_path.c_str());
      if (load_ok != 1)
      {
        setStatus(std::string("failed to load CA bundle: ") + currentSslError());
        SSL_CTX_free(ssl_ctx);
        return false;
      }
    }
    else if (SSL_CTX_set_default_verify_paths(ssl_ctx) != 1)
    {
      setStatus(std::string("failed to load system CA bundle: ") + currentSslError());
      SSL_CTX_free(ssl_ctx);
      return false;
    }
  }
  else
  {
    SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_NONE, nullptr);
  }

  if (!config_.tls_client_key_password.empty())
  {
    SSL_CTX_set_default_passwd_cb(ssl_ctx, sslPasswordCallback);
    SSL_CTX_set_default_passwd_cb_userdata(
        ssl_ctx, const_cast<std::string*>(&config_.tls_client_key_password));
  }

  if (!config_.tls_client_cert_file.empty())
  {
    const int cert_ok =
        SSL_CTX_use_certificate_chain_file(ssl_ctx, config_.tls_client_cert_file.c_str());
    if (cert_ok != 1)
    {
      setStatus(std::string("failed to load client certificate: ") + currentSslError());
      SSL_CTX_free(ssl_ctx);
      return false;
    }
  }

  if (!config_.tls_client_key_file.empty())
  {
    const int key_ok = SSL_CTX_use_PrivateKey_file(
        ssl_ctx, config_.tls_client_key_file.c_str(), SSL_FILETYPE_PEM);
    if (key_ok != 1)
    {
      setStatus(std::string("failed to load client private key: ") + currentSslError());
      SSL_CTX_free(ssl_ctx);
      return false;
    }
  }

  if ((!config_.tls_client_cert_file.empty() && config_.tls_client_key_file.empty()) ||
      (config_.tls_client_cert_file.empty() && !config_.tls_client_key_file.empty()))
  {
    setStatus("both tls_client_cert_file and tls_client_key_file are required for mTLS");
    SSL_CTX_free(ssl_ctx);
    return false;
  }

  if (!config_.tls_client_cert_file.empty() && !config_.tls_client_key_file.empty())
  {
    const int key_match_ok = SSL_CTX_check_private_key(ssl_ctx);
    if (key_match_ok != 1)
    {
      setStatus("client certificate and private key do not match");
      SSL_CTX_free(ssl_ctx);
      return false;
    }
  }

  SSL* ssl = SSL_new(ssl_ctx);
  if (ssl == nullptr)
  {
    setStatus(std::string("failed to create TLS session: ") + currentSslError());
    SSL_CTX_free(ssl_ctx);
    return false;
  }

  const std::string server_name =
      config_.tls_server_name.empty() ? config_.host : config_.tls_server_name;

  if (!server_name.empty())
  {
    SSL_set_tlsext_host_name(ssl, server_name.c_str());
    if (config_.tls_verify_peer)
    {
      X509_VERIFY_PARAM* verify_param = SSL_get0_param(ssl);
      X509_VERIFY_PARAM_set1_host(verify_param, server_name.c_str(), 0U);
    }
  }

  SSL_set_fd(ssl, socket_fd);
  const int connect_ok = SSL_connect(ssl);
  if (connect_ok != 1)
  {
    setStatus(std::string("TLS handshake failed: ") + currentSslError());
    SSL_free(ssl);
    SSL_CTX_free(ssl_ctx);
    return false;
  }

  if (config_.tls_verify_peer && SSL_get_verify_result(ssl) != X509_V_OK)
  {
    setStatus("TLS peer certificate verification failed");
    SSL_free(ssl);
    SSL_CTX_free(ssl_ctx);
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    active_ssl_ctx_ = ssl_ctx;
    active_ssl_ = ssl;
  }

  setStatus("TLS session established");
  return true;
}

void NtripClient::cleanupActiveTransportLocked()
{
  SSL* ssl = active_ssl_;
  SSL_CTX* ssl_ctx = active_ssl_ctx_;
  const int socket_fd = active_socket_;

  active_ssl_ = nullptr;
  active_ssl_ctx_ = nullptr;
  active_socket_ = -1;

  if (ssl != nullptr)
  {
    SSL_shutdown(ssl);
    SSL_free(ssl);
  }

  if (ssl_ctx != nullptr)
  {
    SSL_CTX_free(ssl_ctx);
  }

  if (socket_fd >= 0)
  {
    shutdown(socket_fd, SHUT_RDWR);
    close(socket_fd);
  }
}

bool NtripClient::sendRequest(int socket_fd)
{
  std::ostringstream request;
  request << "GET " << trimMountpoint(config_.mountpoint) << " HTTP/1.1\r\n";
  request << "Host: " << config_.host << ":" << config_.port << "\r\n";
  request << "User-Agent: " << config_.user_agent << "\r\n";
  request << "Ntrip-Version: " << config_.ntrip_version << "\r\n";
  request << "Connection: close\r\n";
  request << "Accept: */*\r\n";
  if (!config_.username.empty())
  {
    request << "Authorization: Basic "
            << base64Encode(config_.username + ":" + config_.password) << "\r\n";
  }
  request << "\r\n";

  if (!sendRaw(socket_fd, request.str()))
  {
    setStatus("failed to send NTRIP request");
    return false;
  }

  setStatus("request sent");
  return true;
}

bool NtripClient::readResponseHeaders(int socket_fd, std::string& headers)
{
  headers.clear();
  std::array<char, 512> buffer{};

  while (running_ && findHeaderEnd(headers) == std::string::npos)
  {
    const ssize_t received = readSome(socket_fd, buffer.data(), buffer.size());
    if (received <= 0)
    {
      setStatus("failed to read response headers");
      return false;
    }
    headers.append(buffer.data(), static_cast<std::size_t>(received));
    if (headers.size() > 8192U)
    {
      setStatus(std::string("response headers exceeded 8KB: ") +
                sanitizeSnippet(headers, 200U));
      return false;
    }
  }

  const std::size_t header_end = findHeaderEnd(headers);
  if (header_end == std::string::npos)
  {
    setStatus("incomplete response headers");
    return false;
  }

  const std::string header_block = headers.substr(0, header_end);
  const bool ok = containsAny(header_block, {"ICY 200 OK", "HTTP/1.0 200 OK", "HTTP/1.1 200 OK"});
  if (!ok)
  {
    if (containsAny(header_block, {"SOURCETABLE 200 OK"}))
    {
      setStatus("received sourcetable response; mountpoint is likely invalid");
    }
    else if (containsAny(header_block, {"401"}))
    {
      setStatus("received unauthorized response; check username, password, and mountpoint");
    }
    else if (containsAny(header_block, {"403"}))
    {
      setStatus("received forbidden response; account or client is not allowed to access this stream");
    }
    else if (containsAny(header_block, {"404"}))
    {
      setStatus("received not-found response; mountpoint or path is likely invalid");
    }
    else if (containsAny(header_block, {"429"}))
    {
      setStatus("received too-many-requests response; caster is rate limiting this client");
    }
    else if (containsAny(header_block, {"502"}))
    {
      setStatus("received bad-gateway response; upstream caster path is unhealthy");
    }
    else if (containsAny(header_block, {"503"}))
    {
      setStatus("received service-unavailable response; caster is temporarily unavailable");
    }
    else if (containsAny(header_block, {"504"}))
    {
      setStatus("received gateway-timeout response; upstream caster path timed out");
    }
    else if (config_.ntrip_version.empty())
    {
      setStatus(std::string("unexpected response; Ntrip-Version was not specified: ") +
                sanitizeSnippet(header_block, 200U));
    }
    else
    {
      setStatus(std::string("unexpected response: ") +
                sanitizeSnippet(header_block, 200U));
    }
    return false;
  }

  const std::string remaining_body = headers.substr(header_end);
  headers = header_block;

  if (!remaining_body.empty())
  {
    processRtcmBytes(reinterpret_cast<const std::uint8_t*>(remaining_body.data()), remaining_body.size());
    dispatchRtcmFrames();
  }

  setStatus("caster accepted stream");

  if (config_.send_initial_gga)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    sendLatestGgaLocked(socket_fd);
  }

  return true;
}

bool NtripClient::streamData(int socket_fd)
{
  std::array<std::uint8_t, 4096> buffer{};
  bool first_rtcm_received = false;
  auto last_rtcm_at = std::chrono::steady_clock::now();

  while (running_)
  {
    const ssize_t received = readSome(socket_fd, buffer.data(), buffer.size());
    if (received == kReadTimeoutResult)
    {
      if (first_rtcm_received)
      {
        const auto now = std::chrono::steady_clock::now();
        const std::chrono::duration<double> elapsed = now - last_rtcm_at;
        if (elapsed.count() >= config_.rtcm_timeout_sec)
        {
          std::ostringstream msg;
          msg << "RTCM data not received for " << std::fixed << std::setprecision(2)
              << config_.rtcm_timeout_sec << " seconds";
          setStatus(msg.str());
          return false;
        }
      }
      continue;
    }
    if (received == 0)
    {
      setStatus("caster closed the connection");
      return false;
    }
    if (received < 0)
    {
      setStatus("stream read failed");
      return false;
    }

    processRtcmBytes(buffer.data(), static_cast<std::size_t>(received));
    if (dispatchRtcmFrames())
    {
      first_rtcm_received = true;
      last_rtcm_at = std::chrono::steady_clock::now();
    }
  }

  return true;
}

bool NtripClient::sleepForSeconds(double seconds) const
{
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::duration<double>(seconds);
  while (running_ && std::chrono::steady_clock::now() < deadline)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return running_;
}

ssize_t NtripClient::readSome(int socket_fd, void* buffer, std::size_t buffer_size)
{
  SSL* ssl = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_socket_ == socket_fd)
    {
      ssl = active_ssl_;
    }
  }

  if (ssl != nullptr)
  {
    while (running_)
    {
      const int rc = SSL_read(ssl, buffer, static_cast<int>(buffer_size));
      if (rc > 0)
      {
        return static_cast<ssize_t>(rc);
      }

      const int ssl_error = SSL_get_error(ssl, rc);
      if (ssl_error == SSL_ERROR_ZERO_RETURN)
      {
        return 0;
      }
      if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE)
      {
        continue;
      }
      if (ssl_error == SSL_ERROR_SYSCALL && (errno == EAGAIN || errno == EWOULDBLOCK))
      {
        return kReadTimeoutResult;
      }

      setStatus(std::string("TLS read failed: ") + currentSslError());
      return -1;
    }
    return -1;
  }

  while (running_)
  {
    const ssize_t received = recv(socket_fd, buffer, buffer_size, 0);
    if (received >= 0)
    {
      return received;
    }
    if (errno == EINTR)
    {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK)
    {
      return kReadTimeoutResult;
    }
    setStatus(std::string("socket read failed: ") + std::strerror(errno));
    return -1;
  }

  return -1;
}

void NtripClient::processRtcmBytes(const std::uint8_t* data, std::size_t size)
{
  if (data == nullptr || size == 0U)
  {
    return;
  }

  rtcm_buffer_.insert(rtcm_buffer_.end(), data, data + size);
  if (rtcm_buffer_.size() > kMaxRtcmBufferSize)
  {
    setStatus("RTCM parser buffer exceeded 10KB; trimming");
    rtcm_buffer_.erase(rtcm_buffer_.begin(),
                       rtcm_buffer_.begin() + (rtcm_buffer_.size() - kMaxRtcmBufferSize));
  }
}

bool NtripClient::dispatchRtcmFrames()
{
  bool published = false;
  std::vector<std::uint8_t> frame;
  while (extractRtcmFrame(frame))
  {
    data_callback_(frame);
    published = true;
  }
  return published;
}

void NtripClient::recordFailureAttempt()
{
  if (!config_.adaptive_reconnect)
  {
    return;
  }

  const auto now = std::chrono::steady_clock::now();
  if (!failure_window_active_)
  {
    failure_window_start_ = now;
    failure_window_active_ = true;
  }

  recent_failure_attempts_.push_back(now);
  const auto burst_window = std::chrono::duration<double>(config_.adaptive_burst_window_sec);
  while (!recent_failure_attempts_.empty() &&
         (now - recent_failure_attempts_.front()) > burst_window)
  {
    recent_failure_attempts_.pop_front();
  }
}

void NtripClient::resetFailureTracking()
{
  recent_failure_attempts_.clear();
  failure_window_active_ = false;
}

double NtripClient::computeAdaptiveMinimumDelaySec() const
{
  if (!config_.adaptive_reconnect)
  {
    return 0.0;
  }

  const auto now = std::chrono::steady_clock::now();
  double minimum_delay_sec = 0.0;

  if (config_.adaptive_burst_max_attempts > 0 &&
      static_cast<int>(recent_failure_attempts_.size()) >= config_.adaptive_burst_max_attempts)
  {
    const auto oldest = recent_failure_attempts_.front();
    const std::chrono::duration<double> elapsed = now - oldest;
    minimum_delay_sec = std::max(minimum_delay_sec,
                                 config_.adaptive_burst_window_sec - elapsed.count());
  }

  if (failure_window_active_)
  {
    const std::chrono::duration<double> failed_for = now - failure_window_start_;
    if (failed_for.count() >= config_.adaptive_slow_after_sec)
    {
      minimum_delay_sec = std::max(minimum_delay_sec, config_.adaptive_slow_interval_sec);
    }
  }

  return std::max(0.0, minimum_delay_sec);
}

bool NtripClient::extractRtcmFrame(std::vector<std::uint8_t>& frame)
{
  while (rtcm_buffer_.size() >= 3U)
  {
    if (rtcm_buffer_.front() != kRtcmPreamble)
    {
      rtcm_buffer_.erase(rtcm_buffer_.begin());
      continue;
    }

    const std::size_t message_length =
        ((static_cast<std::size_t>(rtcm_buffer_[1]) << 8U) |
         static_cast<std::size_t>(rtcm_buffer_[2])) & 0x03FFU;
    const std::size_t packet_length = message_length + 6U;
    if (rtcm_buffer_.size() < packet_length)
    {
      return false;
    }

    const std::uint32_t expected_checksum =
        (static_cast<std::uint32_t>(rtcm_buffer_[packet_length - 3U]) << 16U) |
        (static_cast<std::uint32_t>(rtcm_buffer_[packet_length - 2U]) << 8U) |
        static_cast<std::uint32_t>(rtcm_buffer_[packet_length - 1U]);
    const std::uint32_t actual_checksum =
        computeRtcmChecksum(rtcm_buffer_.data(), packet_length - 3U);
    if (expected_checksum != actual_checksum)
    {
      setStatus("discarding RTCM packet with invalid CRC");
      rtcm_buffer_.erase(rtcm_buffer_.begin());
      continue;
    }

    frame.assign(rtcm_buffer_.begin(), rtcm_buffer_.begin() + packet_length);
    rtcm_buffer_.erase(rtcm_buffer_.begin(), rtcm_buffer_.begin() + packet_length);
    return true;
  }

  return false;
}

std::uint32_t NtripClient::computeRtcmChecksum(const std::uint8_t* data, std::size_t size) const
{
  std::uint32_t crc = 0U;
  for (std::size_t i = 0; i < size; ++i)
  {
    crc = ((crc << 8U) & 0xFFFFFFU) ^
          kRtcmCrcLookup[((crc >> 16U) ^ data[i]) & 0xFFU];
  }
  return crc;
}

ssize_t NtripClient::writeSome(int socket_fd, const void* buffer, std::size_t buffer_size)
{
  SSL* ssl = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_socket_ == socket_fd)
    {
      ssl = active_ssl_;
    }
  }

  if (ssl != nullptr)
  {
    while (running_)
    {
      const int rc = SSL_write(ssl, buffer, static_cast<int>(buffer_size));
      if (rc > 0)
      {
        return static_cast<ssize_t>(rc);
      }

      const int ssl_error = SSL_get_error(ssl, rc);
      if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE)
      {
        continue;
      }

      setStatus(std::string("TLS write failed: ") + currentSslError());
      return -1;
    }
    return -1;
  }

  while (running_)
  {
    const ssize_t sent = send(socket_fd, buffer, buffer_size, 0);
    if (sent >= 0)
    {
      return sent;
    }
    if (errno == EINTR)
    {
      continue;
    }
    setStatus(std::string("socket write failed: ") + std::strerror(errno));
    return -1;
  }

  return -1;
}

bool NtripClient::sendRaw(int socket_fd, const std::string& bytes)
{
  std::size_t total_sent = 0U;
  while (running_ && total_sent < bytes.size())
  {
    const ssize_t sent = writeSome(socket_fd,
                                   bytes.data() + total_sent,
                                   bytes.size() - total_sent);
    if (sent < 0)
    {
      return false;
    }
    total_sent += static_cast<std::size_t>(sent);
  }
  return total_sent == bytes.size();
}

bool NtripClient::sendLatestGgaLocked(int socket_fd)
{
  if (latest_gga_sentence_.empty())
  {
    return false;
  }

  std::string sentence = latest_gga_sentence_;
  if (sentence.back() != '\n')
  {
    sentence += "\r\n";
  }
  return sendRaw(socket_fd, sentence);
}

double NtripClient::computeBackoffDelaySec(int attempt_number) const
{
  const int bounded_attempt = std::max(1, attempt_number);
  const double base_multiplier = std::max(1.0, config_.reconnect_backoff_multiplier);
  const double multiplier_power = std::pow(base_multiplier,
                                           static_cast<double>(bounded_attempt - 1));
  const double raw_delay = config_.reconnect_initial_delay_sec * multiplier_power;
  const double bounded_delay = std::min(config_.reconnect_max_delay_sec, raw_delay);

  static thread_local std::mt19937 generator(std::random_device{}());
  std::uniform_real_distribution<double> jitter(0.9, 1.1);
  return std::max(0.0, bounded_delay * jitter(generator));
}

void NtripClient::setStatus(const std::string& status) const
{
  if (status_callback_)
  {
    status_callback_(status);
  }
}

}  // namespace ros_ntrip_client
