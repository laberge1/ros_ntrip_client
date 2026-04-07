#include "ros_ntrip_client/ntrip_client.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <sstream>
#include <string>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
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
constexpr ssize_t kReadTimeoutResult = -2;

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
  pollfd pfd{};
  pfd.fd = socket_fd;
  pfd.events = write_ready ? POLLOUT : POLLIN;
  const int timeout_ms = static_cast<int>(std::max(0.0, timeout_sec) * 1000.0);
  return poll(&pfd, 1, timeout_ms) > 0;
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

std::string extractStatusLine(const std::string& header_block)
{
  const std::size_t crlf = header_block.find("\r\n");
  if (crlf != std::string::npos)
  {
    return header_block.substr(0, crlf);
  }
  const std::size_t lf = header_block.find('\n');
  if (lf != std::string::npos)
  {
    return header_block.substr(0, lf);
  }
  return header_block;
}

bool statusLineContains(const std::string& status_line, const char* needle)
{
  return status_line.find(needle) != std::string::npos;
}

bool headerContainsCaseInsensitive(const std::string& headers, const std::string& needle)
{
  std::string lower_headers;
  lower_headers.reserve(headers.size());
  for (char ch : headers)
  {
    lower_headers.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
  }
  std::string lower_needle;
  lower_needle.reserve(needle.size());
  for (char ch : needle)
  {
    lower_needle.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
  }
  return lower_headers.find(lower_needle) != std::string::npos;
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

int NtripClient::connectToCaster()
{
  std::call_once(g_openssl_init_once, []()
                 { OPENSSL_init_ssl(0, nullptr); });

  addrinfo hints;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo* addresses = nullptr;
  const std::string port_str = std::to_string(config_.port);
  const int rc = getaddrinfo(config_.host.c_str(), port_str.c_str(), &hints, &addresses);
  if (rc != 0)
  {
    setStatus(StatusCode::DnsFailed, std::string("DNS resolution failed: ") + gai_strerror(rc));
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
        transport_.socket_fd = socket_fd;
      }

      if (!configureTlsForSocket(socket_fd))
      {
        TransportState transport;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          transport = detachActiveTransportLocked();
        }
        cleanupDetachedTransport(transport);
        continue;
      }
    }

    connected_socket = socket_fd;
    break;
  }

  freeaddrinfo(addresses);

  if (connected_socket >= 0)
  {
    setStatus(StatusCode::TcpConnected, "connected to caster");
  }
  else
  {
    setStatus(StatusCode::ConnectFailed, "unable to connect to any resolved address");
  }
  return connected_socket;
}

bool NtripClient::configureTlsForSocket(int socket_fd)
{
  SSL_CTX* ssl_ctx = SSL_CTX_new(TLS_client_method());
  if (ssl_ctx == nullptr)
  {
    setStatus(StatusCode::TlsError, std::string("failed to create TLS context: ") + currentSslError());
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
        setStatus(StatusCode::TlsError, std::string("failed to load CA bundle: ") + currentSslError());
        SSL_CTX_free(ssl_ctx);
        return false;
      }
    }
    else if (SSL_CTX_set_default_verify_paths(ssl_ctx) != 1)
    {
      setStatus(StatusCode::TlsError, std::string("failed to load system CA bundle: ") + currentSslError());
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

  if ((!config_.tls_client_cert_file.empty() && config_.tls_client_key_file.empty()) ||
      (config_.tls_client_cert_file.empty() && !config_.tls_client_key_file.empty()))
  {
    setStatus(StatusCode::TlsError, "both tls_client_cert_file and tls_client_key_file are required for mTLS");
    SSL_CTX_free(ssl_ctx);
    return false;
  }

  if (!config_.tls_client_cert_file.empty())
  {
    const int cert_ok =
        SSL_CTX_use_certificate_chain_file(ssl_ctx, config_.tls_client_cert_file.c_str());
    if (cert_ok != 1)
    {
      setStatus(StatusCode::TlsError, std::string("failed to load client certificate: ") + currentSslError());
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
      setStatus(StatusCode::TlsError, std::string("failed to load client private key: ") + currentSslError());
      SSL_CTX_free(ssl_ctx);
      return false;
    }
  }

  if (!config_.tls_client_cert_file.empty() && !config_.tls_client_key_file.empty())
  {
    const int key_match_ok = SSL_CTX_check_private_key(ssl_ctx);
    if (key_match_ok != 1)
    {
      setStatus(StatusCode::TlsError, "client certificate and private key do not match");
      SSL_CTX_free(ssl_ctx);
      return false;
    }
  }

  SSL* ssl = SSL_new(ssl_ctx);
  if (ssl == nullptr)
  {
    setStatus(StatusCode::TlsError, std::string("failed to create TLS session: ") + currentSslError());
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
    setStatus(StatusCode::TlsError, std::string("TLS handshake failed: ") + currentSslError());
    SSL_free(ssl);
    SSL_CTX_free(ssl_ctx);
    return false;
  }

  if (config_.tls_verify_peer && SSL_get_verify_result(ssl) != X509_V_OK)
  {
    setStatus(StatusCode::TlsError, "TLS peer certificate verification failed");
    SSL_free(ssl);
    SSL_CTX_free(ssl_ctx);
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    transport_.ssl_ctx = ssl_ctx;
    transport_.ssl = ssl;
  }

  setStatus(StatusCode::TlsEstablished, "TLS session established");
  return true;
}

NtripClient::TransportState NtripClient::detachActiveTransportLocked()
{
  TransportState detached = transport_;
  transport_ = TransportState{};
  return detached;
}

void NtripClient::cleanupDetachedTransport(TransportState transport)
{
  if (transport.ssl != nullptr)
  {
    SSL_free(transport.ssl);
  }

  if (transport.ssl_ctx != nullptr)
  {
    SSL_CTX_free(transport.ssl_ctx);
  }

  if (transport.socket_fd >= 0)
  {
    shutdown(transport.socket_fd, SHUT_RDWR);
    close(transport.socket_fd);
  }
}

bool NtripClient::hasPendingTlsReadData(int socket_fd) const
{
  SSL* ssl = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (transport_.socket_fd == socket_fd)
    {
      ssl = transport_.ssl;
    }
  }

  return ssl != nullptr && SSL_pending(ssl) > 0;
}

bool NtripClient::sendRequest(int socket_fd)
{
  const bool is_ntrip_v1 = config_.ntrip_version.empty() ||
                           config_.ntrip_version.find("Ntrip/1") == 0;
  const std::string http_version = is_ntrip_v1 ? "HTTP/1.0" : "HTTP/1.1";

  std::ostringstream request;
  request << "GET " << trimMountpoint(config_.mountpoint) << " " << http_version << "\r\n";
  request << "Host: " << config_.host << ":" << config_.port << "\r\n";
  request << "User-Agent: " << config_.user_agent << "\r\n";
  if (!is_ntrip_v1)
  {
    request << "Ntrip-Version: " << config_.ntrip_version << "\r\n";
  }
  request << "Connection: close\r\n";
  request << "Accept: */*\r\n";
  if (!config_.username.empty())
  {
    request << "Authorization: Basic "
            << base64Encode(config_.username + ":" + config_.password) << "\r\n";
  }
  if (!is_ntrip_v1 && !config_.initial_gga_sentence.empty())
  {
    request << "Ntrip-GGA: " << config_.initial_gga_sentence << "\r\n";
  }
  request << "\r\n";

  if (!sendRaw(socket_fd, request.str()))
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      session_.last_failure_category = FailureCategory::Transport;
    }
    setStatus(StatusCode::RequestFailed, "failed to send NTRIP request");
    return false;
  }

  setStatus(StatusCode::RequestSent, "request sent");
  return true;
}

bool NtripClient::readResponseHeaders(int socket_fd, std::string& headers)
{
  headers.clear();
  std::array<char, 512> buffer{};

  while (running_ && findHeaderEnd(headers) == std::string::npos)
  {
    const ssize_t received = readSome(socket_fd, buffer.data(), buffer.size());
    if (received == kReadTimeoutResult)
    {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        session_.last_failure_category = FailureCategory::Transport;
      }
      setStatus(StatusCode::TransportHeaderFailed, "timed out waiting for caster response headers");
      return false;
    }
    if (received == 0)
    {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        session_.last_failure_category = FailureCategory::Transport;
      }
      setStatus(StatusCode::TransportHeaderFailed, "caster closed the connection before sending response headers");
      return false;
    }
    if (received < 0)
    {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        session_.last_failure_category = FailureCategory::Transport;
      }
      setStatus(StatusCode::TransportHeaderFailed, "failed to read response headers");
      return false;
    }
    headers.append(buffer.data(), static_cast<std::size_t>(received));
    if (headers.size() > 8192U)
    {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        session_.last_failure_category = FailureCategory::Service;
      }
      setStatus(StatusCode::ProtocolError, std::string("response headers exceeded 8KB: ") +
                sanitizeSnippet(headers, 200U));
      return false;
    }
  }

  const std::size_t header_end = findHeaderEnd(headers);
  if (header_end == std::string::npos)
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      session_.last_failure_category = FailureCategory::Service;
    }
    setStatus(StatusCode::ProtocolError, "incomplete response headers");
    return false;
  }

  const std::string header_block = headers.substr(0, header_end);
  const std::string status_line = extractStatusLine(header_block);
  const bool ok = statusLineContains(status_line, "ICY 200 OK") ||
                  statusLineContains(status_line, "HTTP/1.0 200 OK") ||
                  statusLineContains(status_line, "HTTP/1.1 200 OK");
  if (!ok)
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      session_.last_failure_category = FailureCategory::Service;
    }
    if (statusLineContains(status_line, "SOURCETABLE 200 OK"))
    {
      setStatus(StatusCode::MountpointInvalid, "received sourcetable response; mountpoint is likely invalid");
    }
    else if (statusLineContains(status_line, "401"))
    {
      setStatus(StatusCode::AuthFailed, "received unauthorized response; check username, password, and mountpoint");
    }
    else if (statusLineContains(status_line, "403"))
    {
      setStatus(StatusCode::AccessForbidden, "received forbidden response; account or client is not allowed to access this stream");
    }
    else if (statusLineContains(status_line, "404"))
    {
      setStatus(StatusCode::MountpointInvalid, "received not-found response; mountpoint or path is likely invalid");
    }
    else if (statusLineContains(status_line, "429"))
    {
      setStatus(StatusCode::RateLimited, "received too-many-requests response; caster is rate limiting this client");
    }
    else if (statusLineContains(status_line, "502"))
    {
      setStatus(StatusCode::UpstreamError, "received bad-gateway response; upstream caster path is unhealthy");
    }
    else if (statusLineContains(status_line, "503"))
    {
      setStatus(StatusCode::ServiceUnavailable, "received service-unavailable response; caster is temporarily unavailable");
    }
    else if (statusLineContains(status_line, "504"))
    {
      setStatus(StatusCode::UpstreamError, "received gateway-timeout response; upstream caster path timed out");
    }
    else if (config_.ntrip_version.empty())
    {
      setStatus(StatusCode::ProtocolError, std::string("unexpected response; Ntrip-Version was not specified: ") +
                sanitizeSnippet(header_block, 200U));
    }
    else
    {
      setStatus(StatusCode::ProtocolError, std::string("unexpected response: ") +
                sanitizeSnippet(header_block, 200U));
    }
    return false;
  }

  if (headerContainsCaseInsensitive(header_block, "transfer-encoding: chunked"))
  {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      session_.last_failure_category = FailureCategory::Service;
    }
    setStatus(StatusCode::ProtocolError, "chunked transfer encoding is not supported");
    return false;
  }

  const std::string remaining_body = headers.substr(header_end);
  headers = header_block;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    session_.uplink_ready = true;
  }
  setStatus(StatusCode::SessionAccepted, "caster accepted stream");

  if (!remaining_body.empty())
  {
    processRtcmBytes(reinterpret_cast<const std::uint8_t*>(remaining_body.data()), remaining_body.size());
    dispatchRtcmFrames();
  }

  if (config_.send_initial_gga)
  {
    sendCurrentGga(socket_fd);
  }

  return true;
}

ssize_t NtripClient::readSome(int socket_fd, void* buffer, std::size_t buffer_size)
{
  SSL* ssl = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (transport_.socket_fd == socket_fd)
    {
      ssl = transport_.ssl;
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

      setStatus(StatusCode::ReadFailed, std::string("TLS read failed: ") + currentSslError());
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
    setStatus(StatusCode::ReadFailed, std::string("socket read failed: ") + std::strerror(errno));
    return -1;
  }

  return -1;
}

ssize_t NtripClient::writeSome(int socket_fd, const void* buffer, std::size_t buffer_size)
{
  SSL* ssl = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (transport_.socket_fd == socket_fd)
    {
      ssl = transport_.ssl;
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

      setStatus(StatusCode::WriteFailed, std::string("TLS write failed: ") + currentSslError());
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
    setStatus(StatusCode::WriteFailed, std::string("socket write failed: ") + std::strerror(errno));
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

}  // namespace ros_ntrip_client
