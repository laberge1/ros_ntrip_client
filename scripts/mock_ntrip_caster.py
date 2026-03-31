#!/usr/bin/env python3

import argparse
import socket
import threading
import time
import sys


def build_rtcm_payload():
    return bytes([0xD3, 0x00, 0x00, 0x47, 0xEA, 0x4B])


def capture_uplink(conn, capture_file):
    if not capture_file:
        return

    try:
        conn.settimeout(0.2)
        with open(capture_file, "ab") as handle:
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                handle.write(chunk)
                handle.flush()
    except (BrokenPipeError, ConnectionResetError, socket.timeout, OSError):
        return


def handle_client(conn, interval_sec, capture_file):
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

        uplink_thread = None
        if capture_file:
            uplink_thread = threading.Thread(
                target=capture_uplink, args=(conn, capture_file), daemon=True
            )
            uplink_thread.start()

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
    parser.add_argument("--capture-file", default="")
    args, _ = parser.parse_known_args(sys.argv[1:])

    if args.capture_file:
        open(args.capture_file, "wb").close()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((args.host, args.port))
    server.listen(5)

    print("mock_ntrip_caster listening on {}:{}".format(args.host, args.port), flush=True)

    try:
        while True:
            conn, _ = server.accept()
            thread = threading.Thread(
                target=handle_client,
                args=(conn, args.interval_sec, args.capture_file),
                daemon=True,
            )
            thread.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()


if __name__ == "__main__":
    main()
