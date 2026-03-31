#!/usr/bin/env python3

import argparse
import socket
import threading
import time
import sys


def build_rtcm_payload():
    return bytes([0xD3, 0x00, 0x00, 0x47, 0xEA, 0x4B])


def handle_client(conn, interval_sec):
    try:
        conn.settimeout(5.0)
        request = b""
        while b"\r\n\r\n" not in request:
            chunk = conn.recv(4096)
            if not chunk:
                return
            request += chunk

        conn.sendall(b"HTTP/1.1 200 OK\r\n")
        conn.sendall(b"Ntrip-Version: Ntrip/2.0\r\n")
        conn.sendall(b"Content-Type: gnss/data\r\n")
        conn.sendall(b"Connection: close\r\n")
        conn.sendall(b"\r\n")

        payload = build_rtcm_payload()
        while True:
          conn.sendall(payload)
          time.sleep(interval_sec)
    except (BrokenPipeError, ConnectionResetError, socket.timeout):
        return
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Minimal mock NTRIP caster for local smoke tests")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2101)
    parser.add_argument("--interval-sec", type=float, default=1.0)
    args, _ = parser.parse_known_args(sys.argv[1:])

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((args.host, args.port))
    server.listen(5)

    print("mock_ntrip_caster listening on {}:{}".format(args.host, args.port), flush=True)

    try:
        while True:
            conn, _ = server.accept()
            thread = threading.Thread(
                target=handle_client, args=(conn, args.interval_sec), daemon=True
            )
            thread.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()


if __name__ == "__main__":
    main()
