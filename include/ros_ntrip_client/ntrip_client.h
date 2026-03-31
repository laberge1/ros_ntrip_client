#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st SSL;

namespace ros_ntrip_client
{

struct NtripClientConfig
{
  std::string host;
  int port = 2101;
  std::string mountpoint;
  std::string username;
  std::string password;
  std::string user_agent = "NTRIP ros_ntrip_client/0.1";
  std::string ntrip_version = "Ntrip/2.0";
  bool tls_enabled = false;
  bool tls_verify_peer = true;
  std::string tls_server_name;
  std::string tls_ca_cert_file;
  std::string tls_ca_cert_path;
  std::string tls_client_cert_file;
  std::string tls_client_key_file;
  std::string tls_client_key_password;
  double connect_timeout_sec = 10.0;
  double read_timeout_sec = 10.0;
  double session_start_timeout_sec = 15.0;
  double rtcm_timeout_sec = 4.0;
  bool adaptive_reconnect = true;
  int adaptive_burst_max_attempts = 12;
  double adaptive_burst_window_sec = 60.0;
  double adaptive_slow_after_sec = 300.0;
  double adaptive_slow_interval_sec = 300.0;
  double reconnect_initial_delay_sec = 5.0;
  double reconnect_max_delay_sec = 300.0;
  double reconnect_backoff_multiplier = 2.0;
  double transport_reconnect_initial_delay_sec = 1.0;
  double transport_reconnect_max_delay_sec = 10.0;
  double transport_reconnect_backoff_multiplier = 1.5;
  int max_attempts = 0;
  bool send_initial_gga = false;
};

struct NtripClientCounters
{
  std::uint64_t bytes_received = 0U;
  std::uint64_t frames_published = 0U;
  std::uint64_t crc_failures = 0U;
  std::uint64_t discarded_bytes = 0U;
  std::uint64_t buffer_trimmed_bytes = 0U;
};

enum class StatusCode
{
  Info,
  Connecting,
  TcpConnected,
  TlsEstablished,
  RequestSent,
  SessionAccepted,
  StreamActive,
  AuthFailed,
  AccessForbidden,
  MountpointInvalid,
  RateLimited,
  ServiceUnavailable,
  UpstreamError,
  DnsFailed,
  ConnectFailed,
  RequestFailed,
  TransportHeaderFailed,
  ProtocolError,
  TlsError,
  SessionStartTimeout,
  RtcmTimeout,
  StreamClosed,
  SessionEmpty,
  SessionNoValidRtcm,
  Backoff,
  StreamRecovered,
  ReadFailed,
  StoppedMaxAttempts,
  RtcmCrcError,
  RtcmBufferTrimmed
};

struct StatusEvent
{
  StatusCode code = StatusCode::Info;
  std::string message;
};

enum class FailureCategory
{
  None,
  Transport,
  Service
};

class NtripClient
{
public:
  using DataCallback = std::function<void(const std::vector<std::uint8_t>&)>;
  using StatusCallback = std::function<void(const StatusEvent&)>;

  explicit NtripClient(NtripClientConfig config);
  ~NtripClient();

  bool start(DataCallback data_callback, StatusCallback status_callback = StatusCallback());
  void stop();
  void updateGgaSentence(const std::string& gga_sentence);
  NtripClientCounters getCounters() const;

private:
  void workerLoop();
  int connectToCaster();
  bool configureTlsForSocket(int socket_fd);
  void cleanupActiveTransportLocked();
  bool sendRequest(int socket_fd);
  bool readResponseHeaders(int socket_fd, std::string& headers);
  bool streamData(int socket_fd);
  bool sleepForSeconds(double seconds) const;
  ssize_t readSome(int socket_fd, void* buffer, std::size_t buffer_size);
  ssize_t writeSome(int socket_fd, const void* buffer, std::size_t buffer_size);
  bool sendCurrentGga(int socket_fd);
  void processRtcmBytes(const std::uint8_t* data, std::size_t size);
  bool dispatchRtcmFrames();
  bool extractRtcmFrame(std::vector<std::uint8_t>& frame);
  std::uint32_t computeRtcmChecksum(const std::uint8_t* data, std::size_t size) const;
  void recordFailureAttempt();
  void resetFailureTracking();
  double computeAdaptiveMinimumDelaySec() const;
  bool sendRaw(int socket_fd, const std::string& bytes);
  double computeBackoffDelaySec(int attempt_number) const;
  double computeTransportBackoffDelaySec(int attempt_number) const;
  void setStatus(StatusCode code, const std::string& status) const;

  NtripClientConfig config_;
  DataCallback data_callback_;
  StatusCallback status_callback_;

  mutable std::mutex mutex_;
  std::thread worker_thread_;
  std::atomic<bool> running_{false};
  int active_socket_{-1};
  SSL_CTX* active_ssl_ctx_{nullptr};
  SSL* active_ssl_{nullptr};
  std::vector<std::uint8_t> rtcm_buffer_;
  std::deque<std::chrono::steady_clock::time_point> recent_failure_attempts_;
  std::chrono::steady_clock::time_point failure_window_start_{};
  bool failure_window_active_{false};
  bool first_rtcm_frame_received_{false};
  bool stream_active_status_sent_{false};
  bool reconnect_state_reset_for_session_{false};
  FailureCategory last_failure_category_{FailureCategory::None};
  std::chrono::steady_clock::time_point last_rtcm_frame_at_{};
  NtripClientCounters counters_;
  std::uint64_t session_bytes_received_{0U};
  std::uint64_t session_frames_published_{0U};
  std::string latest_gga_sentence_;
};

}  // namespace ros_ntrip_client
