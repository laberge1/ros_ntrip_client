#include "ros_ntrip_client/ntrip_client.h"

#include <cmath>
#include <ctime>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <nmea_msgs/Sentence.h>
#include <ros/ros.h>
#include <rtcm_msgs/Message.h>
#include <std_msgs/String.h>

namespace
{

std::string statusCodeForMessage(const std::string& message)
{
  if (message.find("connection attempt") == 0)
  {
    return "CONNECTING";
  }
  if (message == "connected to caster")
  {
    return "TCP_CONNECTED";
  }
  if (message == "TLS session established")
  {
    return "TLS_ESTABLISHED";
  }
  if (message == "request sent")
  {
    return "REQUEST_SENT";
  }
  if (message == "caster accepted stream")
  {
    return "SESSION_ACCEPTED";
  }
  if (message == "RTCM stream active")
  {
    return "STREAM_ACTIVE";
  }
  if (message.find("received unauthorized response") == 0)
  {
    return "AUTH_FAILED";
  }
  if (message.find("received forbidden response") == 0)
  {
    return "ACCESS_FORBIDDEN";
  }
  if (message.find("received not-found response") == 0 ||
      message.find("received sourcetable response") == 0)
  {
    return "MOUNTPOINT_INVALID";
  }
  if (message.find("received too-many-requests response") == 0)
  {
    return "RATE_LIMITED";
  }
  if (message.find("received service-unavailable response") == 0)
  {
    return "SERVICE_UNAVAILABLE";
  }
  if (message.find("received bad-gateway response") == 0 ||
      message.find("received gateway-timeout response") == 0)
  {
    return "UPSTREAM_ERROR";
  }
  if (message.find("DNS resolution failed") == 0)
  {
    return "DNS_FAILED";
  }
  if (message.find("unable to connect to any resolved address") == 0)
  {
    return "CONNECT_FAILED";
  }
  if (message.find("failed to send NTRIP request") == 0)
  {
    return "REQUEST_FAILED";
  }
  if (message.find("timed out waiting for caster response headers") == 0 ||
      message.find("caster closed the connection before sending response headers") == 0)
  {
    return "TRANSPORT_HEADER_FAILED";
  }
  if (message.find("failed to read response headers") == 0 ||
      message.find("incomplete response headers") == 0 ||
      message.find("response headers exceeded") == 0 ||
      message.find("unexpected response") == 0)
  {
    return "PROTOCOL_ERROR";
  }
  if (message.find("TLS handshake failed") == 0 ||
      message.find("TLS peer certificate verification failed") == 0 ||
      message.find("failed to create TLS context") == 0 ||
      message.find("failed to create TLS session") == 0 ||
      message.find("failed to load CA bundle") == 0 ||
      message.find("failed to load system CA bundle") == 0 ||
      message.find("failed to load client certificate") == 0 ||
      message.find("failed to load client private key") == 0 ||
      message.find("both tls_client_cert_file and tls_client_key_file are required for mTLS") == 0 ||
      message.find("client certificate and private key do not match") == 0)
  {
    return "TLS_ERROR";
  }
  if (message.find("RTCM data not received for") == 0)
  {
    return "RTCM_TIMEOUT";
  }
  if (message.find("RTCM stream did not start within") == 0)
  {
    return "SESSION_START_TIMEOUT";
  }
  if (message == "caster closed the connection")
  {
    return "STREAM_CLOSED";
  }
  if (message == "caster accepted session but closed before sending any stream data")
  {
    return "SESSION_EMPTY";
  }
  if (message == "caster accepted session but no valid RTCM frames were received before close")
  {
    return "SESSION_NO_VALID_RTCM";
  }
  if (message.find("stream disconnected, backing off for") == 0 ||
      message.find("connect failed, backing off for") == 0 ||
      message.find("transport connect failed, backing off for") == 0 ||
      message.find("transport stream disconnected, backing off for") == 0)
  {
    return "BACKOFF";
  }
  if (message == "successful stream established; reconnect state reset")
  {
    return "STREAM_RECOVERED";
  }
  if (message.find("stream read failed") == 0 ||
      message.find("socket read failed") == 0 ||
      message.find("TLS read failed") == 0)
  {
    return "READ_FAILED";
  }
  if (message == "maximum connection attempts reached")
  {
    return "STOPPED_MAX_ATTEMPTS";
  }
  if (message.find("discarding RTCM packet with invalid CRC") == 0)
  {
    return "RTCM_CRC_ERROR";
  }
  if (message.find("RTCM parser buffer exceeded") == 0)
  {
    return "RTCM_BUFFER_TRIMMED";
  }
  return "INFO";
}

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

}  // namespace

int main(int argc, char** argv)
{
  ros::init(argc, argv, "ntrip_client");
  ros::NodeHandle nh;
  ros::NodeHandle pnh("~");

  ros_ntrip_client::NtripClientConfig config;
  pnh.param<std::string>("host", config.host, std::string());
  pnh.param("port", config.port, 2101);
  pnh.param<std::string>("mountpoint", config.mountpoint, std::string());
  pnh.param<std::string>("username", config.username, std::string());
  pnh.param<std::string>("password", config.password, std::string());
  pnh.param<std::string>("user_agent", config.user_agent, std::string("NTRIP ros_ntrip_client/0.1"));
  pnh.param<std::string>("ntrip_version", config.ntrip_version, std::string("Ntrip/2.0"));
  pnh.param("tls_enabled", config.tls_enabled, false);
  pnh.param("tls_verify_peer", config.tls_verify_peer, true);
  pnh.param<std::string>("tls_server_name", config.tls_server_name, std::string());
  pnh.param<std::string>("tls_ca_cert_file", config.tls_ca_cert_file, std::string());
  pnh.param<std::string>("tls_ca_cert_path", config.tls_ca_cert_path, std::string());
  pnh.param<std::string>("tls_client_cert_file", config.tls_client_cert_file, std::string());
  pnh.param<std::string>("tls_client_key_file", config.tls_client_key_file, std::string());
  pnh.param<std::string>("tls_client_key_password", config.tls_client_key_password, std::string());
  pnh.param("connect_timeout_sec", config.connect_timeout_sec, 10.0);
  pnh.param("read_timeout_sec", config.read_timeout_sec, 10.0);
  pnh.param("session_start_timeout_sec", config.session_start_timeout_sec, 15.0);
  pnh.param("rtcm_timeout_sec", config.rtcm_timeout_sec, 4.0);
  pnh.param("adaptive_reconnect", config.adaptive_reconnect, true);
  pnh.param("adaptive_burst_max_attempts", config.adaptive_burst_max_attempts, 12);
  pnh.param("adaptive_burst_window_sec", config.adaptive_burst_window_sec, 60.0);
  pnh.param("adaptive_slow_after_sec", config.adaptive_slow_after_sec, 300.0);
  pnh.param("adaptive_slow_interval_sec", config.adaptive_slow_interval_sec, 300.0);
  pnh.param("reconnect_initial_delay_sec", config.reconnect_initial_delay_sec, 5.0);
  pnh.param("reconnect_max_delay_sec", config.reconnect_max_delay_sec, 300.0);
  pnh.param("reconnect_backoff_multiplier", config.reconnect_backoff_multiplier, 2.0);
  pnh.param("transport_reconnect_initial_delay_sec",
            config.transport_reconnect_initial_delay_sec, 1.0);
  pnh.param("transport_reconnect_max_delay_sec",
            config.transport_reconnect_max_delay_sec, 10.0);
  pnh.param("transport_reconnect_backoff_multiplier",
            config.transport_reconnect_backoff_multiplier, 1.5);
  pnh.param("max_attempts", config.max_attempts, 0);
  pnh.param("send_initial_gga", config.send_initial_gga, false);

  std::string gga_topic;
  std::string rtcm_frame_id;
  bool use_fixed_gga_position = false;
  double gga_send_interval_sec = 0.0;
  double fixed_latitude_deg = 0.0;
  double fixed_longitude_deg = 0.0;
  double fixed_altitude_m = 0.0;
  pnh.param<std::string>("gga_topic", gga_topic, std::string("nmea"));
  pnh.param<std::string>("rtcm_frame_id", rtcm_frame_id, std::string());
  pnh.param("use_fixed_gga_position", use_fixed_gga_position, false);
  pnh.param("gga_send_interval_sec", gga_send_interval_sec, 0.0);
  pnh.param("fixed_latitude_deg", fixed_latitude_deg, 0.0);
  pnh.param("fixed_longitude_deg", fixed_longitude_deg, 0.0);
  pnh.param("fixed_altitude_m", fixed_altitude_m, 0.0);

  if (config.host.empty() || config.mountpoint.empty())
  {
    ROS_FATAL("Parameters '~host' and '~mountpoint' are required.");
    return 1;
  }

  ros::Publisher rtcm_pub = nh.advertise<rtcm_msgs::Message>("rtcm", 10);
  ros::Publisher status_pub = nh.advertise<std_msgs::String>("ntrip_status", 10, true);
  ros::Publisher status_code_pub = nh.advertise<std_msgs::String>("ntrip_status_code", 10, true);
  ros::Publisher counters_pub = nh.advertise<std_msgs::String>("ntrip_counters", 10, true);

  ros_ntrip_client::NtripClient client(config);
  auto latest_gga = std::make_shared<std::string>();

  auto publish_status = [&](const std::string& message)
  {
    std_msgs::String status_msg;
    status_msg.data = message;
    status_pub.publish(status_msg);

    std_msgs::String status_code_msg;
    status_code_msg.data = statusCodeForMessage(message);
    status_code_pub.publish(status_code_msg);

    ROS_INFO_STREAM_THROTTLE(5.0, "NTRIP: " << message);
  };

  auto publish_counters = [&]()
  {
    const ros_ntrip_client::NtripClientCounters counters = client.getCounters();
    std::ostringstream stream;
    stream << "bytes_received=" << counters.bytes_received
           << " frames_published=" << counters.frames_published
           << " crc_failures=" << counters.crc_failures
           << " discarded_bytes=" << counters.discarded_bytes
           << " buffer_trimmed_bytes=" << counters.buffer_trimmed_bytes;

    std_msgs::String counters_msg;
    counters_msg.data = stream.str();
    counters_pub.publish(counters_msg);
  };

  ros::Subscriber gga_sub = nh.subscribe<nmea_msgs::Sentence>(
      gga_topic, 10, [&](const nmea_msgs::Sentence::ConstPtr& msg)
      {
        *latest_gga = msg->sentence;
        client.updateGgaSentence(msg->sentence);
      });

  if (use_fixed_gga_position)
  {
    *latest_gga = positionToGga(fixed_latitude_deg, fixed_longitude_deg, fixed_altitude_m);
    client.updateGgaSentence(*latest_gga);
  }

  ros::Timer counters_timer = nh.createTimer(
      ros::Duration(1.0), [&](const ros::TimerEvent&) { publish_counters(); });

  std::unique_ptr<ros::Timer> gga_timer;
  if (gga_send_interval_sec > 0.0)
  {
    gga_timer.reset(new ros::Timer(
        nh.createTimer(ros::Duration(gga_send_interval_sec),
                       [&, latest_gga, use_fixed_gga_position, fixed_latitude_deg, fixed_longitude_deg,
                        fixed_altitude_m](const ros::TimerEvent&)
                       {
                         if (use_fixed_gga_position)
                         {
                           *latest_gga = positionToGga(
                               fixed_latitude_deg, fixed_longitude_deg, fixed_altitude_m);
                         }

                         if (!latest_gga->empty())
                         {
                           client.updateGgaSentence(*latest_gga);
                         }
                       })));
  }

  const bool started = client.start(
      [&](const std::vector<std::uint8_t>& chunk)
      {
        rtcm_msgs::Message message;
        message.header.stamp = ros::Time::now();
        message.header.frame_id = rtcm_frame_id;
        message.message = chunk;
        rtcm_pub.publish(message);
      },
      publish_status);

  if (!started)
  {
    ROS_FATAL("Failed to start NTRIP client.");
    return 1;
  }

  publish_counters();

  ros::AsyncSpinner spinner(2);
  spinner.start();
  ros::waitForShutdown();
  client.stop();
  return 0;
}
