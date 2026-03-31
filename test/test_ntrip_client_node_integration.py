#!/usr/bin/env python3

import os
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

        deadline = rospy.Time.now() + rospy.Duration(5.0)
        counters = None
        counter_map = {}
        while rospy.Time.now() < deadline:
            counters = rospy.wait_for_message("/ntrip_counters", String, timeout=2.0)
            self.assertIn("bytes_received=", counters.data)
            self.assertIn("frames_published=", counters.data)
            counter_map = {
                item.split("=", 1)[0]: int(item.split("=", 1)[1])
                for item in counters.data.split()
            }
            if counter_map["bytes_received"] > 0 and counter_map["frames_published"] > 0:
                break

        self.assertIsNotNone(counters)
        self.assertGreater(counter_map["bytes_received"], 0)
        self.assertGreater(counter_map["frames_published"], 0)

        rtcm = rospy.wait_for_message("/rtcm", RtcmMessage, timeout=5.0)
        self.assertEqual(rtcm.header.frame_id, "odom")
        self.assertEqual(list(rtcm.message), [0xD3, 0x00, 0x00, 0x47, 0xEA, 0x4B])

        capture_path = "/tmp/ros_ntrip_client_node_integration_gga.log"
        deadline = rospy.Time.now() + rospy.Duration(5.0)
        capture_contents = ""
        while rospy.Time.now() < deadline:
            if os.path.exists(capture_path):
                with open(capture_path, "rb") as handle:
                    capture_contents = handle.read().decode("ascii", errors="ignore")
                if "GPGGA" in capture_contents:
                    break
            rospy.sleep(0.2)
        self.assertIn("GPGGA", capture_contents)


if __name__ == "__main__":
    rospy.init_node("test_ntrip_client_node_integration", anonymous=True)
    rostest.rosrun(
        "ros_ntrip_client",
        "test_ntrip_client_node_integration",
        NtripClientNodeIntegrationTest,
    )
