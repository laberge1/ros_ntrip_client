#include "ros_ntrip_client/ntrip_client.h"
#include "ntrip_client_node_utils.h"

#include <gtest/gtest.h>

#include <ros/ros.h>

#include <string>
#include <vector>

namespace
{

TEST(NtripClientNodeUtilsTest, StatusCodeStringsAreStable)
{
  EXPECT_EQ(ros_ntrip_client::toString(ros_ntrip_client::StatusCode::SessionAccepted),
            "SESSION_ACCEPTED");
  EXPECT_EQ(ros_ntrip_client::toString(ros_ntrip_client::StatusCode::StreamActive),
            "STREAM_ACTIVE");
  EXPECT_EQ(ros_ntrip_client::toString(ros_ntrip_client::StatusCode::RateLimited),
            "RATE_LIMITED");
  EXPECT_EQ(ros_ntrip_client::toString(ros_ntrip_client::StatusCode::SessionNoValidRtcm),
            "SESSION_NO_VALID_RTCM");
}

TEST(NtripClientNodeUtilsTest, PositionToGgaBuildsSentence)
{
  ros::Time::init();
  const std::string gga = ros_ntrip_client::node_utils::positionToGga(53.2724, -9.0539, 12.0);

  EXPECT_FALSE(gga.empty());
  EXPECT_EQ(gga.front(), '$');
  EXPECT_NE(gga.find("GPGGA,"), std::string::npos);
  EXPECT_NE(gga.find(",5316.3440,N,"), std::string::npos);
  EXPECT_NE(gga.find(",00903.2340,W,"), std::string::npos);
  EXPECT_NE(gga.find(",12.00,M,"), std::string::npos);
  EXPECT_NE(gga.find('*'), std::string::npos);
}

TEST(NtripClientNodeUtilsTest, FormatCountersProducesExpectedPayload)
{
  ros_ntrip_client::NtripClientCounters counters;
  counters.bytes_received = 123;
  counters.frames_published = 4;
  counters.crc_failures = 5;
  counters.discarded_bytes = 6;
  counters.buffer_trimmed_bytes = 7;

  EXPECT_EQ(
      ros_ntrip_client::node_utils::formatCounters(counters),
      "bytes_received=123 frames_published=4 crc_failures=5 discarded_bytes=6 buffer_trimmed_bytes=7");
}

TEST(NtripClientNodeUtilsTest, MakeRtcmMessageCopiesPayloadAndFrameId)
{
  const std::vector<std::uint8_t> data = {0xD3, 0x00, 0x00, 0x47, 0xEA, 0x4B};
  const ros::Time stamp(123, 456);

  const rtcm_msgs::Message message =
      ros_ntrip_client::node_utils::makeRtcmMessage(data, "odom", stamp);

  EXPECT_EQ(message.header.stamp, stamp);
  EXPECT_EQ(message.header.frame_id, "odom");
  EXPECT_EQ(message.message, data);
}

}  // namespace
