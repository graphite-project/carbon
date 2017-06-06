#!/usr/bin/python
# -*- coding: utf-8 -*-
""" Copyright 2013 Bryan Irvine
    Copyright 2017 The Graphite Project

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

# Import the precompiled protobuffer. It can be recompiled with:
# $ protoc --python_out=. carbon.proto
from carbon.carbon_pb2 import Payload

import os
import sys
import time
import socket
import struct

CARBON_SERVER = '127.0.0.1'
CARBON_PROTOBUF_PORT = 2005
DELAY = 60


def run(sock, delay):
    """Make the client go go go"""
    while True:
        # Epoch, timestamp in seconds since 1970
        now = int(time.time())

        # Initialize the protobuf payload
        payload_pb = Payload()

        labels = ['1min', '5min', '15min']
        for name, value in zip(labels, os.getloadavg()):
            m = payload_pb.metrics.add()
            m.metric = 'system.loadavg_' + name
            p = m.points.add()
            p.timestamp = now
            p.value = value

        print("sending message")
        print(('-' * 80))
        print(payload_pb)

        package = payload_pb.SerializeToString()

        # The message must be prepended with its size
        size = struct.pack('!L', len(package))
        sock.sendall(size)

        # Then send the actual payload
        sock.sendall(package)

        time.sleep(delay)


def main():
    """Wrap it all up together"""
    delay = DELAY
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.isdigit():
            delay = int(arg)
        else:
            sys.stderr.write(
                    "Ignoring non-integer argument. Using default: %ss\n"
                    % delay)

    sock = socket.socket()
    try:
        sock.connect((CARBON_SERVER, CARBON_PROTOBUF_PORT))
    except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, "
                         "is carbon-cache.py running?" %
                         {'server': CARBON_SERVER,
                          'port': CARBON_PROTOBUF_PORT})

    try:
        run(sock, delay)
    except KeyboardInterrupt:
        sys.stderr.write("\nExiting on CTRL-c\n")
        sys.exit(0)

if __name__ == "__main__":
    main()
