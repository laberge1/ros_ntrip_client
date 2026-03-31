#include "ros_ntrip_client/ntrip_client.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <fcntl.h>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>

#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

namespace ros_ntrip_client
{
namespace
{
constexpr std::size_t kMaxRtcmBufferSize = 10240U;
constexpr ssize_t kReadTimeoutResult = -2;
constexpr std::size_t kMaxCallbackQueueSize = 256U;
}  // namespace

NtripClient::NtripClient(NtripClientConfig config)
  : config_(std::move(config))
{
  session_.rtcm_buffer.reserve(kMaxRtcmBufferSize);
}

NtripClient::~NtripClient()
{
  stop();
  closeWakePipe();
}

bool NtripClient::start(DataCallback data_callback, StatusCallback status_callback)
{
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);

  if (!data_callback)
  {
    setStatus(StatusCode::Info, "refusing to start without a data callback");
    return false;
  }

  if (!ensureWakePipe())
  {
    setStatus(StatusCode::Info, "failed to initialize worker wake pipe");
    return false;
  }

  if (running_)
  {
    setStatus(StatusCode::Info, "client is already running");
    return false;
  }

  if (worker_thread_.joinable())
  {
    if (std::this_thread::get_id() == worker_thread_.get_id())
    {
      setStatus(StatusCode::Info, "cannot restart client from its own callback thread");
      return false;
    }
    worker_thread_.join();
  }

  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true))
  {
    setStatus(StatusCode::Info, "client is already running");
    return false;
  }

  try
  {
    startCallbackDispatcher(std::move(data_callback), std::move(status_callback));
    try
    {
      worker_thread_ = std::thread(&NtripClient::workerLoop, this);
    }
    catch (...)
    {
      running_ = false;
      stopCallbackDispatcher();
      throw;
    }
  }
  catch (...)
  {
    running_ = false;
    throw;
  }
  return true;
}

void NtripClient::stop()
{
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);

  const bool was_running = running_.exchange(false);
  const bool called_from_worker =
      worker_thread_.joinable() && std::this_thread::get_id() == worker_thread_.get_id();

  int socket_to_shutdown = -1;
  if (was_running)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    socket_to_shutdown = transport_.socket_fd;
  }

  if (was_running && socket_to_shutdown >= 0)
  {
    shutdown(socket_to_shutdown, SHUT_RDWR);
  }
  notifyWorker();

  if (worker_thread_.joinable())
  {
    if (called_from_worker)
    {
      return;
    }
    worker_thread_.join();
  }

  TransportState transport;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    transport = detachActiveTransportLocked();
  }
  cleanupDetachedTransport(transport);
  stopCallbackDispatcher();
}

void NtripClient::updateGgaSentence(const std::string& gga_sentence)
{
  bool should_notify = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    session_.latest_gga_sentence = gga_sentence;
    ++session_.latest_gga_generation;
    if (session_.uplink_ready)
    {
      session_.queued_gga_generation = session_.latest_gga_generation;
      should_notify = true;
    }
  }

  if (should_notify)
  {
    notifyWorker();
  }
}

NtripClientCounters NtripClient::getCounters() const
{
  std::lock_guard<std::mutex> lock(mutex_);
  return counters_;
}

void NtripClient::workerLoop()
{
  while (running_)
  {
    int display_attempt = 0;
    bool stopped_max_attempts = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (config_.max_attempts > 0 && reconnect_.total_failed_cycles >= config_.max_attempts)
      {
        stopped_max_attempts = true;
      }
      else
      {
        ++reconnect_.display_attempts;
        display_attempt = reconnect_.display_attempts;
      }
    }

    if (stopped_max_attempts)
    {
      setStatus(StatusCode::StoppedMaxAttempts, "maximum connection attempts reached");
      break;
    }

    std::ostringstream attempt_msg;
    attempt_msg << "connection attempt " << display_attempt;
    setStatus(StatusCode::Connecting, attempt_msg.str());

    int socket_fd = connectToCaster();
    if (socket_fd < 0)
    {
      double delay_sec = 0.0;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        ++reconnect_.total_failed_cycles;
        ++reconnect_.transport_attempts;
        reconnect_.service_attempts = 0;
        delay_sec = computeTransportBackoffDelaySec(reconnect_.transport_attempts);
      }
      std::ostringstream msg;
      msg << "transport connect failed, backing off for " << std::fixed << std::setprecision(2)
          << delay_sec << "s";
      setStatus(StatusCode::Backoff, msg.str());
      if (!sleepForSeconds(delay_sec))
      {
        break;
      }
      continue;
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      transport_.socket_fd = socket_fd;
      resetSessionStateLocked();
    }

    const bool request_sent = sendRequest(socket_fd);
    std::string headers;
    const bool response_ok = request_sent && readResponseHeaders(socket_fd, headers);
    const bool streamed_ok = response_ok && streamData(socket_fd);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (session_.reconnect_state_reset)
      {
        reconnect_.total_failed_cycles = 0;
        reconnect_.service_attempts = 0;
        reconnect_.transport_attempts = 0;
      }
    }

    {
      TransportState transport;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (transport_.socket_fd == socket_fd)
        {
          transport = detachActiveTransportLocked();
        }
      }
      cleanupDetachedTransport(transport);
    }

    if (!running_)
    {
      break;
    }

    if (!streamed_ok)
    {
      FailureCategory failure_category = FailureCategory::Service;
      double delay_sec = 0.0;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        failure_category = session_.last_failure_category;

        ++reconnect_.total_failed_cycles;
        if (failure_category == FailureCategory::Transport)
        {
          ++reconnect_.transport_attempts;
          reconnect_.service_attempts = 0;
          delay_sec = computeTransportBackoffDelaySec(reconnect_.transport_attempts);
        }
        else
        {
          ++reconnect_.service_attempts;
          reconnect_.transport_attempts = 0;
          recordFailureAttempt();
          delay_sec = std::max(computeBackoffDelaySec(reconnect_.service_attempts),
                               computeAdaptiveMinimumDelaySec());
        }
      }
      std::ostringstream msg;

      if (failure_category == FailureCategory::Transport)
      {
        msg << "transport stream disconnected, backing off for " << std::fixed
            << std::setprecision(2) << delay_sec << "s";
      }
      else
      {
        msg << "stream disconnected, backing off for " << std::fixed << std::setprecision(2)
            << delay_sec << "s";
      }

      setStatus(StatusCode::Backoff, msg.str());
      if (!sleepForSeconds(delay_sec))
      {
        break;
      }
      continue;
    }
  }

  running_ = false;
}

void NtripClient::resetSessionStateLocked()
{
  session_.uplink_ready = false;
  session_.queued_gga_generation = 0U;
  session_.first_rtcm_frame_received = false;
  session_.stream_active_status_sent = false;
  session_.reconnect_state_reset = false;
  session_.last_failure_category = FailureCategory::None;
  session_.last_rtcm_frame_at = std::chrono::steady_clock::time_point{};
  session_.bytes_received = 0U;
  session_.frames_published = 0U;
  session_.rtcm_buffer.clear();
}

bool NtripClient::ensureWakePipe()
{
  if (wake_pipe_read_fd_ >= 0 && wake_pipe_write_fd_ >= 0)
  {
    return true;
  }

  int pipe_fds[2] = {-1, -1};
  if (pipe(pipe_fds) != 0)
  {
    return false;
  }

  const int read_flags = fcntl(pipe_fds[0], F_GETFL, 0);
  const int write_flags = fcntl(pipe_fds[1], F_GETFL, 0);
  if (read_flags < 0 || write_flags < 0 ||
      fcntl(pipe_fds[0], F_SETFL, read_flags | O_NONBLOCK) < 0 ||
      fcntl(pipe_fds[1], F_SETFL, write_flags | O_NONBLOCK) < 0)
  {
    close(pipe_fds[0]);
    close(pipe_fds[1]);
    return false;
  }

  wake_pipe_read_fd_ = pipe_fds[0];
  wake_pipe_write_fd_ = pipe_fds[1];
  return true;
}

void NtripClient::closeWakePipe()
{
  if (wake_pipe_read_fd_ >= 0)
  {
    close(wake_pipe_read_fd_);
    wake_pipe_read_fd_ = -1;
  }
  if (wake_pipe_write_fd_ >= 0)
  {
    close(wake_pipe_write_fd_);
    wake_pipe_write_fd_ = -1;
  }
}

void NtripClient::notifyWorker()
{
  if (wake_pipe_write_fd_ < 0)
  {
    return;
  }

  const std::uint8_t byte = 0x01U;
  const ssize_t rc = write(wake_pipe_write_fd_, &byte, sizeof(byte));
  (void)rc;
}

void NtripClient::startCallbackDispatcher(DataCallback data_callback, StatusCallback status_callback)
{
  stopCallbackDispatcher();

  auto dispatcher = std::make_shared<CallbackDispatcherState>();
  dispatcher->data_callback = std::move(data_callback);
  dispatcher->status_callback = std::move(status_callback);
  dispatcher->thread = std::thread(&NtripClient::callbackLoop, dispatcher);
  std::atomic_store(&callback_dispatcher_, std::move(dispatcher));
}

void NtripClient::stopCallbackDispatcher()
{
  std::shared_ptr<CallbackDispatcherState> dispatcher =
      std::atomic_exchange(&callback_dispatcher_, std::shared_ptr<CallbackDispatcherState>{});
  if (!dispatcher)
  {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(dispatcher->mutex);
    dispatcher->stopping = true;
  }
  dispatcher->cv.notify_all();

  if (!dispatcher->thread.joinable())
  {
    return;
  }

  if (std::this_thread::get_id() == dispatcher->thread.get_id())
  {
    dispatcher->thread.detach();
    return;
  }

  dispatcher->thread.join();
}

void NtripClient::callbackLoop(std::shared_ptr<CallbackDispatcherState> dispatcher)
{
  while (true)
  {
    CallbackItem item;
    {
      std::unique_lock<std::mutex> lock(dispatcher->mutex);
      dispatcher->cv.wait(lock, [&]()
                          { return dispatcher->stopping || !dispatcher->queue.empty(); });
      if (dispatcher->queue.empty())
      {
        if (dispatcher->stopping)
        {
          return;
        }
        continue;
      }
      item = std::move(dispatcher->queue.front());
      dispatcher->queue.pop_front();
      dispatcher->cv.notify_all();
    }

    if (item.is_status)
    {
      if (dispatcher->status_callback)
      {
        try
        {
          dispatcher->status_callback(item.status);
        }
        catch (...)
        {
        }
      }
    }
    else if (dispatcher->data_callback)
    {
      try
      {
        dispatcher->data_callback(item.data);
      }
      catch (...)
      {
      }
    }
  }
}

void NtripClient::drainWakePipe()
{
  if (wake_pipe_read_fd_ < 0)
  {
    return;
  }

  std::array<std::uint8_t, 64> buffer{};
  while (read(wake_pipe_read_fd_, buffer.data(), buffer.size()) > 0)
  {
  }
}

bool NtripClient::streamData(int socket_fd)
{
  std::array<std::uint8_t, 4096> buffer{};
  const auto session_started_at = std::chrono::steady_clock::now();

  while (running_)
  {
    if (sendQueuedGgaIfNeeded(socket_fd))
    {
      continue;
    }

    double timeout_sec = config_.session_start_timeout_sec;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto now = std::chrono::steady_clock::now();
      if (session_.first_rtcm_frame_received)
      {
        const std::chrono::duration<double> remaining =
            std::chrono::duration<double>(config_.rtcm_timeout_sec) -
            (now - session_.last_rtcm_frame_at);
        timeout_sec = std::max(0.0, remaining.count());
      }
      else
      {
        const std::chrono::duration<double> remaining =
            std::chrono::duration<double>(config_.session_start_timeout_sec) -
            (now - session_started_at);
        timeout_sec = std::max(0.0, remaining.count());
      }
    }

    const bool tls_read_pending = hasPendingTlsReadData(socket_fd);
    int wait_rc = 1;
    fd_set read_fds;
    FD_ZERO(&read_fds);
    if (!tls_read_pending)
    {
      FD_SET(socket_fd, &read_fds);
      int max_fd = socket_fd;
      if (wake_pipe_read_fd_ >= 0)
      {
        FD_SET(wake_pipe_read_fd_, &read_fds);
        max_fd = std::max(max_fd, wake_pipe_read_fd_);
      }

      timeval timeout{};
      timeout.tv_sec = static_cast<long>(timeout_sec);
      timeout.tv_usec =
          static_cast<long>((timeout_sec - static_cast<double>(timeout.tv_sec)) * 1e6);
      wait_rc = select(max_fd + 1, &read_fds, nullptr, nullptr, &timeout);
    }

    if (wait_rc == 0)
    {
      bool first_rtcm_received = false;
      std::chrono::steady_clock::time_point last_rtcm_at;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        first_rtcm_received = session_.first_rtcm_frame_received;
        last_rtcm_at = session_.last_rtcm_frame_at;
      }

      const auto now = std::chrono::steady_clock::now();
      if (first_rtcm_received)
      {
        const std::chrono::duration<double> elapsed = now - last_rtcm_at;
        if (elapsed.count() >= config_.rtcm_timeout_sec)
        {
          {
            std::lock_guard<std::mutex> lock(mutex_);
            session_.last_failure_category = FailureCategory::Service;
          }
          std::ostringstream msg;
          msg << "RTCM data not received for " << std::fixed << std::setprecision(2)
              << config_.rtcm_timeout_sec << " seconds";
          setStatus(StatusCode::RtcmTimeout, msg.str());
          return false;
        }
      }
      else
      {
        const std::chrono::duration<double> elapsed = now - session_started_at;
        if (elapsed.count() >= config_.session_start_timeout_sec)
        {
          {
            std::lock_guard<std::mutex> lock(mutex_);
            session_.last_failure_category = FailureCategory::Service;
          }
          std::ostringstream msg;
          msg << "RTCM stream did not start within " << std::fixed << std::setprecision(2)
              << config_.session_start_timeout_sec << " seconds";
          setStatus(StatusCode::SessionStartTimeout, msg.str());
          return false;
        }
      }
      continue;
    }

    if (wait_rc < 0)
    {
      if (errno == EINTR)
      {
        continue;
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        session_.last_failure_category = FailureCategory::Transport;
      }
      setStatus(StatusCode::ReadFailed, "stream wait failed");
      return false;
    }

    if (!tls_read_pending && wake_pipe_read_fd_ >= 0 && FD_ISSET(wake_pipe_read_fd_, &read_fds))
    {
      drainWakePipe();
      continue;
    }

    const ssize_t received = readSome(socket_fd, buffer.data(), buffer.size());
    if (received == kReadTimeoutResult)
    {
      continue;
    }
    if (received == 0)
    {
      std::uint64_t session_bytes_received = 0U;
      std::uint64_t session_frames_published = 0U;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        session_bytes_received = session_.bytes_received;
        session_frames_published = session_.frames_published;
      }

      if (session_frames_published == 0U)
      {
        if (session_bytes_received == 0U)
        {
          {
            std::lock_guard<std::mutex> lock(mutex_);
            session_.last_failure_category = FailureCategory::Service;
          }
          setStatus(StatusCode::SessionEmpty, "caster accepted session but closed before sending any stream data");
        }
        else
        {
          {
            std::lock_guard<std::mutex> lock(mutex_);
            session_.last_failure_category = FailureCategory::Service;
          }
          setStatus(StatusCode::SessionNoValidRtcm, "caster accepted session but no valid RTCM frames were received before close");
        }
      }
      else
      {
        {
          std::lock_guard<std::mutex> lock(mutex_);
          session_.last_failure_category = FailureCategory::Transport;
        }
        setStatus(StatusCode::StreamClosed, "caster closed the connection");
      }
      return false;
    }
    if (received < 0)
    {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        session_.last_failure_category = FailureCategory::Transport;
      }
      setStatus(StatusCode::ReadFailed, "stream read failed");
      return false;
    }

    processRtcmBytes(buffer.data(), static_cast<std::size_t>(received));
    dispatchRtcmFrames();
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

bool NtripClient::sendQueuedGgaIfNeeded(int socket_fd)
{
  std::uint64_t queued_generation = 0U;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!session_.uplink_ready || session_.queued_gga_generation == 0U)
    {
      return false;
    }
    queued_generation = session_.queued_gga_generation;
  }

  const bool sent = sendCurrentGga(socket_fd);
  if (!sent)
  {
    return false;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  return session_.queued_gga_generation != queued_generation;
}

void NtripClient::recordFailureAttempt()
{
  if (!config_.adaptive_reconnect)
  {
    return;
  }

  const auto now = std::chrono::steady_clock::now();
  if (!reconnect_.failure_window_active)
  {
    reconnect_.failure_window_start = now;
    reconnect_.failure_window_active = true;
  }

  reconnect_.recent_failure_attempts.push_back(now);
  const auto burst_window = std::chrono::duration<double>(config_.adaptive_burst_window_sec);
  while (!reconnect_.recent_failure_attempts.empty() &&
         (now - reconnect_.recent_failure_attempts.front()) > burst_window)
  {
    reconnect_.recent_failure_attempts.pop_front();
  }
}

void NtripClient::resetFailureTracking()
{
  reconnect_.recent_failure_attempts.clear();
  reconnect_.failure_window_active = false;
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
      static_cast<int>(reconnect_.recent_failure_attempts.size()) >= config_.adaptive_burst_max_attempts)
  {
    const auto oldest = reconnect_.recent_failure_attempts.front();
    const std::chrono::duration<double> elapsed = now - oldest;
    minimum_delay_sec = std::max(minimum_delay_sec,
                                 config_.adaptive_burst_window_sec - elapsed.count());
  }

  if (reconnect_.failure_window_active)
  {
    const std::chrono::duration<double> failed_for = now - reconnect_.failure_window_start;
    if (failed_for.count() >= config_.adaptive_slow_after_sec)
    {
      minimum_delay_sec = std::max(minimum_delay_sec, config_.adaptive_slow_interval_sec);
    }
  }

  return std::max(0.0, minimum_delay_sec);
}

bool NtripClient::sendCurrentGga(int socket_fd)
{
  std::string sentence;
  std::uint64_t generation = 0U;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_.latest_gga_sentence.empty())
    {
      return false;
    }
    sentence = session_.latest_gga_sentence;
    generation = session_.latest_gga_generation;
  }

  if (sentence.empty())
  {
    return false;
  }

  if (sentence.back() != '\n')
  {
    sentence += "\r\n";
  }
  if (!sendRaw(socket_fd, sentence))
  {
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_.queued_gga_generation == generation)
    {
      session_.queued_gga_generation = 0U;
    }
  }
  return true;
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

double NtripClient::computeTransportBackoffDelaySec(int attempt_number) const
{
  const int bounded_attempt = std::max(1, attempt_number);
  const double base_multiplier =
      std::max(1.0, config_.transport_reconnect_backoff_multiplier);
  const double multiplier_power = std::pow(base_multiplier,
                                           static_cast<double>(bounded_attempt - 1));
  const double raw_delay = config_.transport_reconnect_initial_delay_sec * multiplier_power;
  const double bounded_delay = std::min(config_.transport_reconnect_max_delay_sec, raw_delay);

  static thread_local std::mt19937 generator(std::random_device{}());
  std::uniform_real_distribution<double> jitter(0.9, 1.1);
  return std::max(0.0, bounded_delay * jitter(generator));
}

void NtripClient::enqueueDataCallback(std::vector<std::uint8_t> data) const
{
  const std::shared_ptr<CallbackDispatcherState> dispatcher = std::atomic_load(&callback_dispatcher_);
  if (!dispatcher)
  {
    return;
  }

  {
    std::unique_lock<std::mutex> lock(dispatcher->mutex);
    dispatcher->cv.wait(lock, [&]()
                        {
                          return dispatcher->stopping || !running_ ||
                                 dispatcher->queue.size() < kMaxCallbackQueueSize;
                        });
    if (dispatcher->stopping || !running_)
    {
      return;
    }
    dispatcher->queue.push_back(CallbackItem{false, std::move(data), StatusEvent{}});
  }
  dispatcher->cv.notify_one();
}

void NtripClient::setStatus(StatusCode code, const std::string& status) const
{
  const std::shared_ptr<CallbackDispatcherState> dispatcher = std::atomic_load(&callback_dispatcher_);
  if (!dispatcher || !dispatcher->status_callback)
  {
    return;
  }

  {
    std::unique_lock<std::mutex> lock(dispatcher->mutex);
    dispatcher->cv.wait(lock, [&]()
                        {
                          return dispatcher->stopping || !running_ ||
                                 dispatcher->queue.size() < kMaxCallbackQueueSize;
                        });
    if (dispatcher->stopping || !running_)
    {
      return;
    }
    dispatcher->queue.push_back(CallbackItem{true, {}, StatusEvent{code, status}});
  }
  dispatcher->cv.notify_one();
}

}  // namespace ros_ntrip_client
