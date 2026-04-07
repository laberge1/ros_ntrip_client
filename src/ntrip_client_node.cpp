#include "ntrip_client_node_utils.h"

#include <mutex>
#include <memory>
#include <sstream>
#include <vector>

#include <nmea_msgs/Sentence.h>
#include <ros/ros.h>
#include <rtcm_msgs/Message.h>
#include <std_msgs/String.h>

int main(int argc, char** argv)
{
  ros::init(argc, argv, "ntrip_client");
  ros::NodeHandle nh;
  ros::NodeHandle pnh("~");

  const ros_ntrip_client::node_utils::NodeConfig node_config =
      ros_ntrip_client::node_utils::loadNodeConfig(pnh);
  const ros_ntrip_client::NtripClientConfig& config = node_config.client_config;

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
  auto latest_gga_mutex = std::make_shared<std::mutex>();

  auto publish_status = [&](const ros_ntrip_client::StatusEvent& event)
  {
    std_msgs::String status_msg;
    status_msg.data = event.message;
    status_pub.publish(status_msg);

    std_msgs::String status_code_msg;
    status_code_msg.data = ros_ntrip_client::toString(event.code);
    status_code_pub.publish(status_code_msg);

    ROS_INFO_STREAM_THROTTLE(5.0, "NTRIP: " << event.message);
  };

  auto publish_counters = [&]()
  {
    const ros_ntrip_client::NtripClientCounters counters = client.getCounters();
    std_msgs::String counters_msg;
    counters_msg.data = ros_ntrip_client::node_utils::formatCounters(counters);
    counters_pub.publish(counters_msg);
  };

  ros::Subscriber gga_sub = nh.subscribe<nmea_msgs::Sentence>(
      node_config.gga_topic, 10, [&](const nmea_msgs::Sentence::ConstPtr& msg)
      {
        {
          std::lock_guard<std::mutex> lock(*latest_gga_mutex);
          *latest_gga = msg->sentence;
        }
        client.updateGgaSentence(msg->sentence);
      });

  if (node_config.use_fixed_gga_position)
  {
    const std::string fixed_gga = ros_ntrip_client::node_utils::positionToGga(
        node_config.fixed_latitude_deg,
        node_config.fixed_longitude_deg,
        node_config.fixed_altitude_m);
    {
      std::lock_guard<std::mutex> lock(*latest_gga_mutex);
      *latest_gga = fixed_gga;
    }
    client.updateGgaSentence(fixed_gga);
  }

  ros::Timer counters_timer = nh.createTimer(
      ros::Duration(1.0), [&](const ros::TimerEvent&) { publish_counters(); });

  std::unique_ptr<ros::Timer> gga_timer;
  if (node_config.gga_send_interval_sec > 0.0)
  {
    gga_timer = std::make_unique<ros::Timer>(
        nh.createTimer(ros::Duration(node_config.gga_send_interval_sec),
                       [&, latest_gga, latest_gga_mutex](const ros::TimerEvent&)
                       {
                         std::string gga_to_send;
                         if (node_config.use_fixed_gga_position)
                         {
                           gga_to_send = ros_ntrip_client::node_utils::positionToGga(
                               node_config.fixed_latitude_deg,
                               node_config.fixed_longitude_deg,
                               node_config.fixed_altitude_m);
                           std::lock_guard<std::mutex> lock(*latest_gga_mutex);
                           *latest_gga = gga_to_send;
                         }
                         else
                         {
                           std::lock_guard<std::mutex> lock(*latest_gga_mutex);
                           gga_to_send = *latest_gga;
                         }

                         if (!gga_to_send.empty())
                         {
                           client.updateGgaSentence(gga_to_send);
                         }
                       })));
  }

  const bool started = client.start(
      [&](const std::vector<std::uint8_t>& chunk)
      {
        const rtcm_msgs::Message message = ros_ntrip_client::node_utils::makeRtcmMessage(
            chunk, node_config.rtcm_frame_id, ros::Time::now());
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
