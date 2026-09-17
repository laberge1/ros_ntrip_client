#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st SSL;

class NtripClientTestAccess;

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
  double rtcm_timeout_sec = 10.0;
  bool adaptive_reconnect = true;
  int adaptive_burst_max_attempts = 12;
  double adaptive_burst_window_sec = 60.0;
  double adaptive_slow_after_sec = 300.0;
  double adaptive_slow_interval_sec = 300.0;
  double reconnect_initial_delay_sec = 5.0;
  double reconnect_max_delay_sec = 300.0;
  double reconnect_backoff_multiplier = 2.0;
  double transport_reconnect_initial_delay_sec = 5.0;
  double transport_reconnect_max_delay_sec = 10.0;
  double transport_reconnect_backoff_multiplier = 1.5;
  int max_attempts = 0;
  bool send_initial_gga = true;
  std::string initial_gga_sentence;
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
  WriteFailed,
  StoppedMaxAttempts,
  RtcmCrcError,
  RtcmBufferTrimmed
};

struct StatusEvent
{
  StatusCode code = StatusCode::Info;
  std::string message;
};

std::string toString(StatusCode code);

enum class FailureCategory
{
  None,
  Transport,
  Service
};

class NtripClient
{
  friend class ::NtripClientTestAccess;

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
  struct CallbackItem
  {
    bool is_status = false;
    std::vector<std::uint8_t> data;
    StatusEvent status;
  };

  struct CallbackDispatcherState
  {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<CallbackItem> queue;
    DataCallback data_callback;
    StatusCallback status_callback;
    bool stopping = false;
    std::thread thread;
  };

  struct TransportState
  {
    int socket_fd = -1;
    SSL_CTX* ssl_ctx = nullptr;
    SSL* ssl = nullptr;
  };

  struct SessionState
  {
    enum class ChunkDecodeState { Size, Data, Terminator };
    std::vector<std::uint8_t> rtcm_buffer;
    std::vector<std::uint8_t> chunk_line_buffer;
    std::size_t chunk_bytes_remaining = 0U;
    std::size_t chunk_terminator_bytes_seen = 0U;
    ChunkDecodeState chunk_decode_state = ChunkDecodeState::Size;
    bool response_is_chunked = false;
    bool uplink_ready = false;
    std::uint64_t queued_gga_generation = 0U;
    std::uint64_t latest_gga_generation = 0U;
    bool first_rtcm_frame_received = false;
    bool stream_active_status_sent = false;
    bool reconnect_state_reset = false;
    FailureCategory last_failure_category = FailureCategory::None;
    std::chrono::steady_clock::time_point last_rtcm_frame_at{};
    std::uint64_t bytes_received = 0U;
    std::uint64_t frames_published = 0U;
    std::string latest_gga_sentence;
  };

  struct ReconnectState
  {
    std::deque<std::chrono::steady_clock::time_point> recent_failure_attempts;
    std::chrono::steady_clock::time_point failure_window_start{};
    bool failure_window_active = false;
    int display_attempts = 0;
    int total_failed_cycles = 0;
    int service_attempts = 0;
    int transport_attempts = 0;
  };

  void workerLoop();
  int connectToCaster();
  bool configureTlsForSocket(int socket_fd);
  TransportState detachActiveTransportLocked();
  void cleanupDetachedTransport(TransportState transport);
  bool sendRequest(int socket_fd);
  bool readResponseHeaders(int socket_fd, std::string& headers);
  bool streamData(int socket_fd);
  bool sleepForSeconds(double seconds);
  bool ensureWakePipe();
  void closeWakePipe();
  void notifyWorker();
  void drainWakePipe();
  void startCallbackDispatcher(DataCallback data_callback, StatusCallback status_callback);
  void stopCallbackDispatcher();
  static void callbackLoop(std::shared_ptr<CallbackDispatcherState> dispatcher);
  bool hasPendingTlsReadData(int socket_fd) const;
  bool sendQueuedGgaIfNeeded(int socket_fd);
  ssize_t readSome(int socket_fd, void* buffer, std::size_t buffer_size);
  ssize_t writeSome(int socket_fd, const void* buffer, std::size_t buffer_size);
  bool sendCurrentGga(int socket_fd);
  void processResponseBodyBytes(const std::uint8_t* data, std::size_t size);
  void processRtcmBytes(const std::uint8_t* data, std::size_t size);
  bool dispatchRtcmFrames();
  bool extractRtcmFrame(std::vector<std::uint8_t>& frame);
  std::uint32_t computeRtcmChecksum(const std::uint8_t* data, std::size_t size) const;
  void recordFailureAttempt();
  void resetFailureTracking();
  void resetSessionStateLocked();
  double computeAdaptiveMinimumDelaySec() const;
  bool sendRaw(int socket_fd, const std::string& bytes);
  double computeBackoffDelaySec(int attempt_number) const;
  double computeTransportBackoffDelaySec(int attempt_number) const;
  void enqueueDataCallback(std::vector<std::uint8_t> data) const;
  void setStatus(StatusCode code, const std::string& status) const;

  NtripClientConfig config_;
  mutable std::mutex lifecycle_mutex_;
  mutable std::mutex mutex_;
  std::thread worker_thread_;
  std::atomic<bool> running_{false};
  NtripClientCounters counters_;
  TransportState transport_;
  SessionState session_;
  ReconnectState reconnect_;
  std::shared_ptr<CallbackDispatcherState> callback_dispatcher_;
  int wake_pipe_read_fd_{-1};
  int wake_pipe_write_fd_{-1};
};

}  // namespace ros_ntrip_client
