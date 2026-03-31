#include "ros_ntrip_client/ntrip_client.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>

#include <sys/socket.h>
#include <unistd.h>

namespace ros_ntrip_client
{
namespace
{
constexpr std::size_t kMaxRtcmBufferSize = 10240U;
constexpr ssize_t kReadTimeoutResult = -2;
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
    setStatus(StatusCode::Info, "refusing to start without a data callback");
    return false;
  }

  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true))
  {
    setStatus(StatusCode::Info, "client is already running");
    return false;
  }

  data_callback_ = std::move(data_callback);
  status_callback_ = std::move(status_callback);
  worker_thread_ = std::thread(&NtripClient::workerLoop, this);
  return true;
}

void NtripClient::stop()
{
  const bool was_running = running_.exchange(false);

  int socket_to_close = -1;
  if (was_running)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    socket_to_close = active_socket_;
  }

  if (was_running && socket_to_close >= 0)
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
  int socket_fd = -1;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    latest_gga_sentence_ = gga_sentence;
    if (active_socket_ >= 0)
    {
      socket_fd = active_socket_;
    }
  }

  if (socket_fd >= 0)
  {
    sendCurrentGga(socket_fd);
  }
}

NtripClientCounters NtripClient::getCounters() const
{
  std::lock_guard<std::mutex> lock(mutex_);
  return counters_;
}

void NtripClient::workerLoop()
{
  int display_attempts = 0;
  int total_failed_cycles = 0;
  int service_attempts = 0;
  int transport_attempts = 0;

  while (running_)
  {
    if (config_.max_attempts > 0 && total_failed_cycles >= config_.max_attempts)
    {
      setStatus(StatusCode::StoppedMaxAttempts, "maximum connection attempts reached");
      break;
    }

    ++display_attempts;
    std::ostringstream attempt_msg;
    attempt_msg << "connection attempt " << display_attempts;
    setStatus(StatusCode::Connecting, attempt_msg.str());

    int socket_fd = connectToCaster();
    if (socket_fd < 0)
    {
      ++total_failed_cycles;
      ++transport_attempts;
      service_attempts = 0;
      const double delay_sec = computeTransportBackoffDelaySec(transport_attempts);
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
      active_socket_ = socket_fd;
      first_rtcm_frame_received_ = false;
      stream_active_status_sent_ = false;
      reconnect_state_reset_for_session_ = false;
      last_failure_category_ = FailureCategory::None;
      last_rtcm_frame_at_ = std::chrono::steady_clock::time_point{};
      session_bytes_received_ = 0U;
      session_frames_published_ = 0U;
    }
    rtcm_buffer_.clear();

    const bool request_sent = sendRequest(socket_fd);
    std::string headers;
    const bool response_ok = request_sent && readResponseHeaders(socket_fd, headers);
    const bool streamed_ok = response_ok && streamData(socket_fd);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (reconnect_state_reset_for_session_)
      {
        total_failed_cycles = 0;
        service_attempts = 0;
        transport_attempts = 0;
      }
    }

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
      FailureCategory failure_category = FailureCategory::Service;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        failure_category = last_failure_category_;
      }

      ++total_failed_cycles;
      double delay_sec = 0.0;
      std::ostringstream msg;

      if (failure_category == FailureCategory::Transport)
      {
        ++transport_attempts;
        service_attempts = 0;
        delay_sec = computeTransportBackoffDelaySec(transport_attempts);
        msg << "transport stream disconnected, backing off for " << std::fixed
            << std::setprecision(2) << delay_sec << "s";
      }
      else
      {
        ++service_attempts;
        transport_attempts = 0;
        recordFailureAttempt();
        delay_sec = std::max(computeBackoffDelaySec(service_attempts),
                             computeAdaptiveMinimumDelaySec());
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

bool NtripClient::streamData(int socket_fd)
{
  std::array<std::uint8_t, 4096> buffer{};
  const auto session_started_at = std::chrono::steady_clock::now();

  while (running_)
  {
    const ssize_t received = readSome(socket_fd, buffer.data(), buffer.size());
    if (received == kReadTimeoutResult)
    {
      bool first_rtcm_received = false;
      std::chrono::steady_clock::time_point last_rtcm_at;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        first_rtcm_received = first_rtcm_frame_received_;
        last_rtcm_at = last_rtcm_frame_at_;
      }

      const auto now = std::chrono::steady_clock::now();
      if (first_rtcm_received)
      {
        const std::chrono::duration<double> elapsed = now - last_rtcm_at;
        if (elapsed.count() >= config_.rtcm_timeout_sec)
        {
          {
            std::lock_guard<std::mutex> lock(mutex_);
            last_failure_category_ = FailureCategory::Service;
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
            last_failure_category_ = FailureCategory::Service;
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
    if (received == 0)
    {
      std::uint64_t session_bytes_received = 0U;
      std::uint64_t session_frames_published = 0U;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        session_bytes_received = session_bytes_received_;
        session_frames_published = session_frames_published_;
      }

      if (session_frames_published == 0U)
      {
        if (session_bytes_received == 0U)
        {
          {
            std::lock_guard<std::mutex> lock(mutex_);
            last_failure_category_ = FailureCategory::Service;
          }
          setStatus(StatusCode::SessionEmpty, "caster accepted session but closed before sending any stream data");
        }
        else
        {
          {
            std::lock_guard<std::mutex> lock(mutex_);
            last_failure_category_ = FailureCategory::Service;
          }
          setStatus(StatusCode::SessionNoValidRtcm, "caster accepted session but no valid RTCM frames were received before close");
        }
      }
      else
      {
        {
          std::lock_guard<std::mutex> lock(mutex_);
          last_failure_category_ = FailureCategory::Transport;
        }
        setStatus(StatusCode::StreamClosed, "caster closed the connection");
      }
      return false;
    }
    if (received < 0)
    {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        last_failure_category_ = FailureCategory::Transport;
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

bool NtripClient::sendCurrentGga(int socket_fd)
{
  std::string sentence;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (latest_gga_sentence_.empty())
    {
      return false;
    }
    sentence = latest_gga_sentence_;
  }

  if (sentence.empty())
  {
    return false;
  }

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

void NtripClient::setStatus(StatusCode code, const std::string& status) const
{
  if (status_callback_)
  {
    status_callback_(StatusEvent{code, status});
  }
}

}  // namespace ros_ntrip_client
