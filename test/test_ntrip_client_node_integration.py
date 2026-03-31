#!/usr/bin/env python3

import unittest

import rospy
import rostest
from rtcm_msgs.msg import Message as RtcmMessage
from std_msgs.msg import String


class NtripClientNodeIntegrationTest(unittest.TestCase):
    def test_node_publishes_status_counters_and_rtcm(self):
        deadline = rospy.Time.now() + rospy.Duration(10.0)
        status_code = None
        while rospy.Time.now() < deadline:
            status_code = rospy.wait_for_message("/ntrip_status_code", String, timeout=2.0)
            if status_code.data == "STREAM_ACTIVE":
                break
        self.assertIsNotNone(status_code)
        self.assertEqual(status_code.data, "STREAM_ACTIVE")

        deadline = rospy.Time.now() + rospy.Duration(5.0)
        status_text = None
        while rospy.Time.now() < deadline:
            status_text = rospy.wait_for_message("/ntrip_status", String, timeout=2.0)
            if "RTCM stream active" in status_text.data:
                break
        self.assertIsNotNone(status_text)
        self.assertIn("RTCM stream active", status_text.data)

        counters = rospy.wait_for_message("/ntrip_counters", String, timeout=5.0)
        self.assertIn("bytes_received=", counters.data)
        self.assertIn("frames_published=", counters.data)

        rtcm = rospy.wait_for_message("/rtcm", RtcmMessage, timeout=5.0)
        self.assertEqual(rtcm.header.frame_id, "odom")
        self.assertEqual(list(rtcm.message), [0xD3, 0x00, 0x00, 0x47, 0xEA, 0x4B])


if __name__ == "__main__":
    rospy.init_node("test_ntrip_client_node_integration", anonymous=True)
    rostest.rosrun(
        "ros_ntrip_client",
        "test_ntrip_client_node_integration",
        NtripClientNodeIntegrationTest,
    )
