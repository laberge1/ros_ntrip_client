#include "ntrip_client_node_utils.h"

#include <cmath>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>

namespace ros_ntrip_client
{
namespace node_utils
{
namespace
{

std::string computeNmeaChecksumPayload(const std::string& payload)
{
  unsigned char checksum = 0U;
  for (unsigned char ch : payload)
  {
    checksum ^= ch;
  }

  std::ostringstream sentence;
  sentence << "$" << payload << "*" << std::uppercase << std::hex << std::setw(2)
           << std::setfill('0') << static_cast<int>(checksum);
  return sentence.str();
}

std::string formatLatitude(double latitude_deg)
{
  const double abs_lat = std::fabs(latitude_deg);
  const int degrees = static_cast<int>(abs_lat);
  const double minutes = (abs_lat - degrees) * 60.0;

  std::ostringstream stream;
  stream << std::setw(2) << std::setfill('0') << degrees
         << std::setw(7) << std::fixed << std::setprecision(4) << minutes;
  return stream.str();
}

std::string formatLongitude(double longitude_deg)
{
  const double abs_lon = std::fabs(longitude_deg);
  const int degrees = static_cast<int>(abs_lon);
  const double minutes = (abs_lon - degrees) * 60.0;

  std::ostringstream stream;
  stream << std::setw(3) << std::setfill('0') << degrees
         << std::setw(7) << std::fixed << std::setprecision(4) << minutes;
  return stream.str();
}

}  // namespace

NodeConfig loadNodeConfig(ros::NodeHandle& private_nh)
{
  NodeConfig config;

  private_nh.param<std::string>("host", config.client_config.host, std::string());
  private_nh.param("port", config.client_config.port, 2101);
  private_nh.param<std::string>("mountpoint", config.client_config.mountpoint, std::string());
  private_nh.param<std::string>("username", config.client_config.username, std::string());
  private_nh.param<std::string>("password", config.client_config.password, std::string());
  private_nh.param<std::string>(
      "user_agent", config.client_config.user_agent, std::string("NTRIP ros_ntrip_client/0.1"));
  private_nh.param<std::string>(
      "ntrip_version", config.client_config.ntrip_version, std::string("Ntrip/2.0"));
  private_nh.param("tls_enabled", config.client_config.tls_enabled, false);
  private_nh.param("tls_verify_peer", config.client_config.tls_verify_peer, true);
  private_nh.param<std::string>(
      "tls_server_name", config.client_config.tls_server_name, std::string());
  private_nh.param<std::string>(
      "tls_ca_cert_file", config.client_config.tls_ca_cert_file, std::string());
  private_nh.param<std::string>(
      "tls_ca_cert_path", config.client_config.tls_ca_cert_path, std::string());
  private_nh.param<std::string>(
      "tls_client_cert_file", config.client_config.tls_client_cert_file, std::string());
  private_nh.param<std::string>(
      "tls_client_key_file", config.client_config.tls_client_key_file, std::string());
  private_nh.param<std::string>(
      "tls_client_key_password", config.client_config.tls_client_key_password, std::string());
  private_nh.param("connect_timeout_sec", config.client_config.connect_timeout_sec, 10.0);
  private_nh.param("read_timeout_sec", config.client_config.read_timeout_sec, 10.0);
  private_nh.param(
      "session_start_timeout_sec", config.client_config.session_start_timeout_sec, 15.0);
  private_nh.param("rtcm_timeout_sec", config.client_config.rtcm_timeout_sec, 4.0);
  private_nh.param("adaptive_reconnect", config.client_config.adaptive_reconnect, true);
  private_nh.param(
      "adaptive_burst_max_attempts", config.client_config.adaptive_burst_max_attempts, 12);
  private_nh.param(
      "adaptive_burst_window_sec", config.client_config.adaptive_burst_window_sec, 60.0);
  private_nh.param(
      "adaptive_slow_after_sec", config.client_config.adaptive_slow_after_sec, 300.0);
  private_nh.param(
      "adaptive_slow_interval_sec", config.client_config.adaptive_slow_interval_sec, 300.0);
  private_nh.param(
      "reconnect_initial_delay_sec", config.client_config.reconnect_initial_delay_sec, 5.0);
  private_nh.param(
      "reconnect_max_delay_sec", config.client_config.reconnect_max_delay_sec, 300.0);
  private_nh.param(
      "reconnect_backoff_multiplier", config.client_config.reconnect_backoff_multiplier, 2.0);
  private_nh.param("transport_reconnect_initial_delay_sec",
                    config.client_config.transport_reconnect_initial_delay_sec,
                    1.0);
  private_nh.param("transport_reconnect_max_delay_sec",
                    config.client_config.transport_reconnect_max_delay_sec,
                    10.0);
  private_nh.param("transport_reconnect_backoff_multiplier",
                    config.client_config.transport_reconnect_backoff_multiplier,
                    1.5);
  private_nh.param("max_attempts", config.client_config.max_attempts, 0);
  private_nh.param("send_initial_gga", config.client_config.send_initial_gga, false);

  private_nh.param<std::string>("gga_topic", config.gga_topic, std::string("nmea"));
  private_nh.param<std::string>("rtcm_frame_id", config.rtcm_frame_id, std::string());
  private_nh.param("use_fixed_gga_position", config.use_fixed_gga_position, false);
  private_nh.param("gga_send_interval_sec", config.gga_send_interval_sec, 0.0);
  private_nh.param("fixed_latitude_deg", config.fixed_latitude_deg, 0.0);
  private_nh.param("fixed_longitude_deg", config.fixed_longitude_deg, 0.0);
  private_nh.param("fixed_altitude_m", config.fixed_altitude_m, 0.0);

  return config;
}

std::string positionToGga(double latitude, double longitude, double altitude)
{
  const ros::Time stamp = ros::Time::now();
  const std::time_t seconds = static_cast<std::time_t>(stamp.sec);
  const std::tm utc = *std::gmtime(&seconds);

  std::ostringstream time_stream;
  time_stream << std::setw(2) << std::setfill('0') << utc.tm_hour
              << std::setw(2) << std::setfill('0') << utc.tm_min;

  std::ostringstream seconds_stream;
  seconds_stream << std::setw(5) << std::setfill('0')
                 << std::fixed << std::setprecision(2)
                 << (static_cast<double>(utc.tm_sec) + stamp.nsec * 1e-9);
  time_stream << seconds_stream.str();

  std::ostringstream payload;
  payload << "GPGGA,"
          << time_stream.str() << ","
          << formatLatitude(latitude) << "," << (latitude >= 0.0 ? "N" : "S") << ","
          << formatLongitude(longitude) << "," << (longitude >= 0.0 ? "E" : "W") << ","
          << "1,12,1.0,"
          << std::fixed << std::setprecision(2) << altitude << ",M,0.0,M,,";

  return computeNmeaChecksumPayload(payload.str());
}

std::string formatCounters(const NtripClientCounters& counters)
{
  std::ostringstream stream;
  stream << "bytes_received=" << counters.bytes_received
         << " frames_published=" << counters.frames_published
         << " crc_failures=" << counters.crc_failures
         << " discarded_bytes=" << counters.discarded_bytes
         << " buffer_trimmed_bytes=" << counters.buffer_trimmed_bytes;
  return stream.str();
}

rtcm_msgs::Message makeRtcmMessage(
    const std::vector<std::uint8_t>& data,
    const std::string& frame_id,
    const ros::Time& stamp)
{
  rtcm_msgs::Message message;
  message.header.stamp = stamp;
  message.header.frame_id = frame_id;
  message.message = data;
  return message;
}

}  // namespace node_utils
}  // namespace ros_ntrip_client
