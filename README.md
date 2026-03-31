# ros_ntrip_client

ROS Noetic package providing:

- A reusable C++ NTRIP client library.
- A ROS node that connects to an NTRIP caster and publishes validated RTCM frames on `rtcm`.

## Compliance-oriented behavior

The client is designed to avoid abusive reconnect loops:

- Configurable `connect_timeout_sec` and `read_timeout_sec`
- Configurable `max_attempts` (`0` means retry indefinitely)
- Configurable exponential backoff with jitter:
  - `reconnect_initial_delay_sec`
  - `reconnect_max_delay_sec`
  - `reconnect_backoff_multiplier`
- Separate lighter transport backoff for network-fabric failures and post-stream dropouts:
  - `transport_reconnect_initial_delay_sec`
  - `transport_reconnect_max_delay_sec`
  - `transport_reconnect_backoff_multiplier`
- Standards-friendly headers:
  - `User-Agent`
  - `Ntrip-Version`
  - `Authorization: Basic ...` when credentials are provided
- Optional TLS transport with peer verification enabled by default

## ROS interfaces

Publishes:

- `rtcm` (`rtcm_msgs/Message`)
- `ntrip_status` (`std_msgs/String`)
- `ntrip_status_code` (`std_msgs/String`)
- `ntrip_counters` (`std_msgs/String`)

Subscribes:

- `nmea` (`nmea_msgs/Sentence`) for externally generated GGA sentences

Optional fixed-position GGA generation:

- `~rtcm_frame_id`
- `~use_fixed_gga_position`
- `~fixed_latitude_deg`
- `~fixed_longitude_deg`
- `~fixed_altitude_m`
- `~gga_send_interval_sec`

## Parameters

See [config/ntrip_client.yaml](config/ntrip_client.yaml).

Required:

- `~host`
- `~mountpoint`

Important anti-ban controls:

- `~connect_timeout_sec`
- `~read_timeout_sec`
- `~session_start_timeout_sec`
- `~rtcm_timeout_sec`
- `~adaptive_reconnect`
- `~adaptive_burst_max_attempts`
- `~adaptive_burst_window_sec`
- `~adaptive_slow_after_sec`
- `~adaptive_slow_interval_sec`
- `~reconnect_initial_delay_sec`
- `~reconnect_max_delay_sec`
- `~reconnect_backoff_multiplier`
- `~transport_reconnect_initial_delay_sec`
- `~transport_reconnect_max_delay_sec`
- `~transport_reconnect_backoff_multiplier`
- `~max_attempts`

TLS controls:

- `~tls_enabled`
- `~tls_verify_peer`
- `~tls_server_name`
- `~tls_ca_cert_file`
- `~tls_ca_cert_path`
- `~tls_client_cert_file`
- `~tls_client_key_file`
- `~tls_client_key_password`

GGA resend control:

- `~gga_send_interval_sec`

Set `gga_send_interval_sec` to `0.0` to disable periodic resend. For VRS or `NEAR` mountpoints, values between `1.0` and `10.0` seconds are typical.

Adaptive reconnect control:

- `~adaptive_reconnect`
- `~adaptive_burst_max_attempts`
- `~adaptive_burst_window_sec`
- `~adaptive_slow_after_sec`
- `~adaptive_slow_interval_sec`

With the defaults, the client keeps the normal exponential backoff but also avoids exceeding about `12` failed attempts in `60` seconds, and after `300` seconds of continuous failure it slows to one reconnect every `300` seconds.

Transport failures are handled separately from caster-side failures. TCP connect failures and disconnects after a stream has already become active use the lighter `transport_reconnect_*` backoff and do not advance the service-side adaptive reconnect history. This is intended to recover faster from Wi-Fi roaming and other network-fabric interruptions without becoming aggressive toward public casters.

Structured status:

- `ntrip_status` remains the human-readable status text
- `ntrip_status_code` publishes a machine-readable code such as:
  - `SESSION_ACCEPTED`
  - `STREAM_ACTIVE`
  - `SESSION_START_TIMEOUT`
  - `SESSION_EMPTY`
  - `SESSION_NO_VALID_RTCM`
  - `TRANSPORT_HEADER_FAILED`
  - `AUTH_FAILED`
  - `MOUNTPOINT_INVALID`
  - `RATE_LIMITED`
  - `RTCM_TIMEOUT`
  - `BACKOFF`
- `ntrip_counters` publishes parser and transport counters such as:
  - `bytes_received`
  - `frames_published`
  - `crc_failures`
  - `discarded_bytes`
  - `buffer_trimmed_bytes`

## Launch

```bash
roslaunch ros_ntrip_client ntrip_client.launch
```

## Example Launches

These examples assume a fresh workspace at `~/ntrip_ws` built as described below.

### 1. Local RTK Base NTRIP Server

Use this for a caster on your own LAN where aggressive reconnect pacing is acceptable.

```bash
cd ~/ntrip_ws
source /opt/ros/noetic/setup.bash
source devel/setup.bash
rosrun ros_ntrip_client ntrip_client_node \
  _host:=192.168.96.60 \
  _port:=2101 \
  _mountpoint:=rtk \
  _username:=rtk \
  _password:=rtk \
  _connect_timeout_sec:=2.0 \
  _read_timeout_sec:=3.0 \
  _rtcm_timeout_sec:=3.0 \
  _adaptive_reconnect:=false \
  _reconnect_initial_delay_sec:=1.0 \
  _reconnect_max_delay_sec:=5.0 \
  _reconnect_backoff_multiplier:=1.5
```

If the local caster requires GGA:

```bash
rosrun ros_ntrip_client ntrip_client_node \
  _host:=192.168.96.60 \
  _port:=2101 \
  _mountpoint:=rtk \
  _username:=rtk \
  _password:=rtk \
  _use_fixed_gga_position:=true \
  _fixed_latitude_deg:=53.2724 \
  _fixed_longitude_deg:=-9.0539 \
  _fixed_altitude_m:=12.0 \
  _send_initial_gga:=true \
  _gga_send_interval_sec:=5.0
```

### 2. SAPOS

This assumes a VRS-style SAPOS mountpoint that requires GGA. Replace host, mountpoint, username, password, and coordinates with the details from your SAPOS provider and your rover's actual position. The example coordinates below are the Nürburgring.

```bash
cd ~/ntrip_ws
source /opt/ros/noetic/setup.bash
source devel/setup.bash
rosrun ros_ntrip_client ntrip_client_node \
  _host:=YOUR_SAPOS_HOSTNAME \
  _port:=2101 \
  _mountpoint:=YOUR_HEPS_OR_RHEPS_MOUNTPOINT \
  _username:=YOUR_USERNAME \
  _password:=YOUR_PASSWORD \
  _use_fixed_gga_position:=true \
  _fixed_latitude_deg:=50.333909 \
  _fixed_longitude_deg:=6.947060 \
  _fixed_altitude_m:=620.0 \
  _send_initial_gga:=true \
  _gga_send_interval_sec:=5.0 \
  _connect_timeout_sec:=5.0 \
  _read_timeout_sec:=8.0 \
  _rtcm_timeout_sec:=8.0 \
  _adaptive_reconnect:=true \
  _reconnect_initial_delay_sec:=5.0 \
  _reconnect_max_delay_sec:=60.0 \
  _reconnect_backoff_multiplier:=2.0
```

### 3. CentipedeRTK

The public Centipede documentation describes `crtk.net:2101`, `centipede/centipede`, and `NEAR` for closest-base selection. `NEAR` requires GGA. Replace the coordinates with your rover's actual position. The example below uses the Centre Pompidou.

```bash
cd ~/ntrip_ws
source /opt/ros/noetic/setup.bash
source devel/setup.bash
rosrun ros_ntrip_client ntrip_client_node \
  _host:=crtk.net \
  _port:=2101 \
  _mountpoint:=NEAR \
  _username:=centipede \
  _password:=centipede \
  _send_initial_gga:=true \
  _gga_send_interval_sec:=5.0 \
  _use_fixed_gga_position:=true \
  _fixed_latitude_deg:=48.860786 \
  _fixed_longitude_deg:=2.352858 \
  _fixed_altitude_m:=222.0 \
  _adaptive_reconnect:=true \
  _reconnect_initial_delay_sec:=5.0 \
  _reconnect_max_delay_sec:=120.0
```

### 4. RTK2go

RTK2go is a global community caster. It expects a valid email as username and typically ignores the password, which is commonly set to `none`. Replace the coordinates with your rover's actual position. The example below uses the Royal Observatory Greenwich.

```bash
cd ~/ntrip_ws
source /opt/ros/noetic/setup.bash
source devel/setup.bash
rosrun ros_ntrip_client ntrip_client_node \
  _host:=rtk2go.com \
  _port:=2101 \
  _mountpoint:=YOUR_MOUNTPOINT \
  _username:=your-email@example.com \
  _password:=none \
  _use_fixed_gga_position:=true \
  _fixed_latitude_deg:=51.477811 \
  _fixed_longitude_deg:=-0.001475 \
  _fixed_altitude_m:=46.0 \
  _send_initial_gga:=true \
  _gga_send_interval_sec:=5.0 \
  _adaptive_reconnect:=true \
  _reconnect_initial_delay_sec:=10.0 \
  _reconnect_max_delay_sec:=300.0 \
  _reconnect_backoff_multiplier:=2.0
```

For RTK2go TLS:

```bash
rosrun ros_ntrip_client ntrip_client_node \
  _host:=rtk2go.com \
  _port:=2102 \
  _mountpoint:=YOUR_MOUNTPOINT \
  _username:=your-email@example.com \
  _password:=none \
  _tls_enabled:=true \
  _use_fixed_gga_position:=true \
  _fixed_latitude_deg:=51.477811 \
  _fixed_longitude_deg:=-0.001475 \
  _fixed_altitude_m:=46.0 \
  _send_initial_gga:=true \
  _gga_send_interval_sec:=5.0
```

## Suggested Config By Service

These are starting points, not guarantees. Tune them for your rover, network quality, and the caster's own documentation.

### 1. Local or Self-Hosted NTRIP

Use this when the caster is on your LAN, under your control, and ban risk is negligible.

- `connect_timeout_sec: 2.0`
- `read_timeout_sec: 3.0`
- `rtcm_timeout_sec: 3.0`
- `reconnect_initial_delay_sec: 1.0`
- `reconnect_max_delay_sec: 5.0`
- `reconnect_backoff_multiplier: 1.5`
- `max_attempts: 0`
- `send_initial_gga: true` only if the mountpoint requires GGA

Rationale:

- Favor fast failure detection and fast recovery
- Ban risk is low because you control the caster

### 2. SAPOS

SAPOS is regional and service details vary by state. The examples in public documentation show VRS-style HEPS/R-HEPS services over NTRIP, RTCM 3.2+, and in some regions multiple redundant casters. VRS mountpoints typically require a valid GGA position.

- `connect_timeout_sec: 5.0`
- `read_timeout_sec: 8.0`
- `rtcm_timeout_sec: 8.0`
- `reconnect_initial_delay_sec: 5.0`
- `reconnect_max_delay_sec: 60.0`
- `reconnect_backoff_multiplier: 2.0`
- `max_attempts: 0`
- `send_initial_gga: true`
- `gga_send_interval_sec: 5.0`

Rationale:

- SAPOS is generally a government or contract service rather than an anonymous public sandbox
- Use conservative retries, especially on paid accounts
- Prefer hostnames over numeric IPs when the provider recommends it

Examples from public SAPOS documentation:

- GeoNord HEPS / R-HEPS: NTRIP, RTCM 3.2+, 1 Hz, VRS service
- Sachsen-Anhalt HEPS: recommended hostnames, redundant casters, example credentials `user/user`

### 3. RTK2go

RTK2go explicitly reserves the right to block users for abuse. Its current public documentation says:

- users should log in with a valid email as username
- password is ignored and may be `none`
- both Rev1 and Rev2 are supported
- temporary bans can occur after a few hundred failed connection attempts
- normal bans are about 3 hours, with longer bans for repeat or aggressive offenders

Suggested settings:

- `connect_timeout_sec: 5.0`
- `read_timeout_sec: 10.0`
- `rtcm_timeout_sec: 10.0`
- `reconnect_initial_delay_sec: 10.0`
- `reconnect_max_delay_sec: 300.0`
- `reconnect_backoff_multiplier: 2.0`
- `max_attempts: 0`
- `send_initial_gga: true` for NEAR/VRS-style streams, otherwise as required by the stream
- `gga_send_interval_sec: 5.0` for NEAR/VRS-style streams

Notes:

- username should be a valid email
- password should generally be `none`
- TLS is available on port `2102`

### 4. CentipedeRTK

Centipede documents public NTRIP access on `crtk.net:2101`, login `centipede`, password `centipede`, with `NEAR` for automatic closest-base selection. `NEAR` requires the client to send GGA.

Suggested settings:

- `connect_timeout_sec: 5.0`
- `read_timeout_sec: 8.0`
- `rtcm_timeout_sec: 8.0`
- `reconnect_initial_delay_sec: 5.0`
- `reconnect_max_delay_sec: 120.0`
- `reconnect_backoff_multiplier: 2.0`
- `max_attempts: 0`
- `send_initial_gga: true` when using `NEAR`
- `gga_send_interval_sec: 5.0` when using `NEAR`

Rationale:

- Centipede is free and public, but its AUP allows service access to be blocked for non-compliant use
- stay conservative with reconnect pacing

### 5. EUREF-IP / BKG / Other Public Research Broadcasters

These are real NTRIP services, but they are generally not RTK correction services for rover positioning. Public documentation for the ROB EUREF-IP broadcaster explicitly says its streams are raw GNSS data, not RTK streams, and are unsuitable for operational RTK positioning.

Use this client against them only if you are intentionally consuming raw NTRIP streams, not rover corrections.

Suggested settings:

- `connect_timeout_sec: 5.0`
- `read_timeout_sec: 10.0`
- `rtcm_timeout_sec: 15.0`
- `reconnect_initial_delay_sec: 10.0`
- `reconnect_max_delay_sec: 300.0`
- `reconnect_backoff_multiplier: 2.0`
- `max_attempts: 0`

### 6. Generic Public Caster Default

If you do not know the operator's ban policy, start here.

- `connect_timeout_sec: 5.0`
- `read_timeout_sec: 8.0`
- `rtcm_timeout_sec: 8.0`
- `reconnect_initial_delay_sec: 5.0`
- `reconnect_max_delay_sec: 300.0`
- `reconnect_backoff_multiplier: 2.0`
- `max_attempts: 0`
- `send_initial_gga: true` only when the mountpoint needs it and the rover already has a usable position
- `gga_send_interval_sec: 5.0` for VRS/NEAR-style streams

Rationale:

- aligns with conservative public-caster behavior
- avoids fast reconnect loops that many operators interpret as abuse

## Public Caster Etiquette

The RTCM best-practices paper for NTRIP clients recommends:

- do not reconnect repeatedly after an explicit error without fixing the cause
- use an increasing delay between failed attempts
- never reconnect more than about 12 times in 60 seconds
- after extended failures, slow down to no more than about once every 5 minutes
- do not connect unless the client is actually ready to operate

For VRS or NEAR mountpoints, do not connect until you can provide a valid GGA if the service requires it.

## Public Service References

- RTK2go how-to and ban language: <https://rtk2go.com/how-to-connect/>
- RTK2go front page and TLS port: <https://rtk2go.com/>
- RTCM NTRIP client practices paper: <https://agrilab.unilasalle.fr/projets/attachments/download/5952/2023-SC104-1344-NTRIP-Client-Practices.pdf>
- Centipede connection details: <https://docs.centipede.fr/docs/proprietaire/>
- Centipede AUP / CGU: <https://docs.centipede.fr/docs/centipede/CGU.html>
- SAPOS GeoNord HEPS: <https://sapos.geonord.de/dienste/heps>
- SAPOS GeoNord R-HEPS: <https://sapos.geonord.de/dienste/r-heps-sh>
- SAPOS Sachsen-Anhalt HEPS NTRIP access: <https://www.lvermgeo.sachsen-anhalt.de/de/gdp-heps-korrekturdatenabgabe.html>
- EUREF-IP ROB broadcaster notice: <https://euref-ip.oma.be/>
- BKG / EUREF-IP registration terms: <https://register.rtcm-ntrip.org/cgi-bin/registration.cgi>

## Build In Isolation

Create a clean catkin workspace and clone this package into `src`:

```bash
mkdir -p ~/ntrip_ws/src
cd ~/ntrip_ws/src
git clone https://github.com/olliewalsh/ros_ntrip_client.git
cd ~/ntrip_ws
source /opt/ros/noetic/setup.bash
rosdep install --from-paths src --ignore-src -r -y
catkin_make
source devel/setup.bash
```

Expected external ROS dependencies:

- `nmea_msgs`
- `rtcm_msgs`
- `roscpp`
- `std_msgs`
- OpenSSL development headers

## Smoke Test In Isolation

This package includes a minimal mock caster at `scripts/mock_ntrip_caster.py`. It is a transport smoke test only. It verifies connection, reconnect, topic publication, and optional GGA forwarding. It does not validate RTCM payload correctness.

Terminal 1:

```bash
source /opt/ros/noetic/setup.bash
roscore
```

Terminal 2:

```bash
cd ~/ntrip_ws/src/ros_ntrip_client
python3 scripts/mock_ntrip_caster.py --host 127.0.0.1 --port 2101
```

Terminal 3:

```bash
cd ~/ntrip_ws
source /opt/ros/noetic/setup.bash
source devel/setup.bash
rosrun ros_ntrip_client ntrip_client_node \
  _host:=127.0.0.1 \
  _port:=2101 \
  _mountpoint:=TEST \
  _tls_enabled:=false \
  _use_fixed_gga_position:=true \
  _fixed_latitude_deg:=53.2724 \
  _fixed_longitude_deg:=-9.0539 \
  _fixed_altitude_m:=12.0 \
  _send_initial_gga:=true
```

Terminal 4:

```bash
cd ~/ntrip_ws
source /opt/ros/noetic/setup.bash
source devel/setup.bash
rostopic echo /ntrip_status
```

Terminal 5:

```bash
cd ~/ntrip_ws
source /opt/ros/noetic/setup.bash
source devel/setup.bash
rostopic echo /rtcm
```

What to expect:

- `/ntrip_status` should report connection attempts, connection success, and stream acceptance
- `/rtcm` should publish repeated `rtcm_msgs/Message` messages
- if you stop the mock caster and restart it, the node should reconnect with backoff

## Manual Checks

To test upstream NMEA forwarding instead of fixed coordinates:

```bash
rostopic pub /nmea nmea_msgs/Sentence \
  "{header: {stamp: now, frame_id: ''}, sentence: '$GPGGA,123519,4807.038,N,01131.000,E,1,12,1.0,545.4,M,46.9,M,,*47'}"
```

To test reconnect pacing:

- set `reconnect_initial_delay_sec` to a small value such as `2.0`
- set `reconnect_max_delay_sec` to a bounded value such as `10.0`
- optionally set `transport_reconnect_initial_delay_sec` to `1.0` and `transport_reconnect_max_delay_sec` to `5.0` for roaming-style testing
- stop the mock caster and watch `/ntrip_status`
- confirm reconnect attempts slow down instead of hammering the server

## Run Unit Tests

The package also includes a small gtest suite for the C++ client library. These tests cover:

- successful RTCM startup
- header timeout before session acceptance
- accepted session that never produces a first RTCM frame
- accepted session that closes without sending stream data

From a fresh catkin workspace:

```bash
mkdir -p ~/ntrip_ws/src
cd ~/ntrip_ws/src
git clone https://github.com/olliewalsh/ros_ntrip_client.git
cd ..
source /opt/ros/noetic/setup.bash
rosdep install --from-paths src --ignore-src -r -y
catkin_make run_tests_ros_ntrip_client_ntrip_client_test
catkin_test_results build
```

If your catkin version uses the gtest-specific target naming, use:

```bash
catkin_make run_tests_ros_ntrip_client_gtest_ntrip_client_test
catkin_test_results build
```

To build the test target without running it:

```bash
cd ~/ntrip_ws
source /opt/ros/noetic/setup.bash
catkin_make --pkg ros_ntrip_client tests
```
