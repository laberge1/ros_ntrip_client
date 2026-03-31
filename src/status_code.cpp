#include "ros_ntrip_client/ntrip_client.h"

#include <string>

namespace ros_ntrip_client
{

std::string toString(StatusCode code)
{
  switch (code)
  {
    case StatusCode::Connecting: return "CONNECTING";
    case StatusCode::TcpConnected: return "TCP_CONNECTED";
    case StatusCode::TlsEstablished: return "TLS_ESTABLISHED";
    case StatusCode::RequestSent: return "REQUEST_SENT";
    case StatusCode::SessionAccepted: return "SESSION_ACCEPTED";
    case StatusCode::StreamActive: return "STREAM_ACTIVE";
    case StatusCode::AuthFailed: return "AUTH_FAILED";
    case StatusCode::AccessForbidden: return "ACCESS_FORBIDDEN";
    case StatusCode::MountpointInvalid: return "MOUNTPOINT_INVALID";
    case StatusCode::RateLimited: return "RATE_LIMITED";
    case StatusCode::ServiceUnavailable: return "SERVICE_UNAVAILABLE";
    case StatusCode::UpstreamError: return "UPSTREAM_ERROR";
    case StatusCode::DnsFailed: return "DNS_FAILED";
    case StatusCode::ConnectFailed: return "CONNECT_FAILED";
    case StatusCode::RequestFailed: return "REQUEST_FAILED";
    case StatusCode::TransportHeaderFailed: return "TRANSPORT_HEADER_FAILED";
    case StatusCode::ProtocolError: return "PROTOCOL_ERROR";
    case StatusCode::TlsError: return "TLS_ERROR";
    case StatusCode::SessionStartTimeout: return "SESSION_START_TIMEOUT";
    case StatusCode::RtcmTimeout: return "RTCM_TIMEOUT";
    case StatusCode::StreamClosed: return "STREAM_CLOSED";
    case StatusCode::SessionEmpty: return "SESSION_EMPTY";
    case StatusCode::SessionNoValidRtcm: return "SESSION_NO_VALID_RTCM";
    case StatusCode::Backoff: return "BACKOFF";
    case StatusCode::StreamRecovered: return "STREAM_RECOVERED";
    case StatusCode::ReadFailed: return "READ_FAILED";
    case StatusCode::StoppedMaxAttempts: return "STOPPED_MAX_ATTEMPTS";
    case StatusCode::RtcmCrcError: return "RTCM_CRC_ERROR";
    case StatusCode::RtcmBufferTrimmed: return "RTCM_BUFFER_TRIMMED";
    case StatusCode::Info: return "INFO";
  }

  return "INFO";
}

}  // namespace ros_ntrip_client
