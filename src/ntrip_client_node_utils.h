#pragma once

#include <string>

#include <ros/ros.h>

#include "ros_ntrip_client/ntrip_client.h"

namespace ros_ntrip_client
{
namespace node_utils
{

struct NodeConfig
{
  NtripClientConfig client_config;
  std::string gga_topic = "nmea";
  std::string rtcm_frame_id;
  bool use_fixed_gga_position = false;
  double gga_send_interval_sec = 0.0;
  double fixed_latitude_deg = 0.0;
  double fixed_longitude_deg = 0.0;
  double fixed_altitude_m = 0.0;
};

NodeConfig loadNodeConfig(ros::NodeHandle& private_nh);
std::string positionToGga(double latitude, double longitude, double altitude);

}  // namespace node_utils
}  // namespace ros_ntrip_client
