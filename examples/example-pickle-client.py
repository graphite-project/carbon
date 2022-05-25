#!/usr/bin/python
"""Copyright 2013 Bryan Irvine

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import re
import sys
import time
import socket
import platform
import subprocess
import pickle
import struct

CARBON_SERVER = '127.0.0.1'
CARBON_PICKLE_PORT = 2004
DELAY = 60

def get_loadavg():
    """
    Get the load average for a unix-like system.
    For more details, "man proc" and "man uptime"
    """
    if platform.system() == "Linux":
        return open('/proc/loadavg').read().split()[:3]
    else:
        command = "uptime"
        process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        stdout = process.communicate()[0].strip()
        # Split on whitespace and commas
        output = re.split("[\s,]+", stdout)
        return output[-3:]

def run(sock, delay):
    """Make the client go go go"""
    while True:
        now = int(time.time())
        tuples = ([])
        lines = []
        #We're gonna report all three loadavg values
        loadavg = get_loadavg()
        tuples.append(('system.loadavg_1min', (now,loadavg[0])))
        tuples.append(('system.loadavg_5min', (now,loadavg[1])))
        tuples.append(('system.loadavg_15min', (now,loadavg[2])))
        lines.append("system.loadavg_1min %s %d" % (loadavg[0], now))
        lines.append("system.loadavg_5min %s %d" % (loadavg[1], now))
        lines.append("system.loadavg_15min %s %d" % (loadavg[2], now))
        message = '\n'.join(lines) + '\n' #all lines must end in a newline
        print("sending message")
        print('-' * 80)
        print(message)
        package = pickle.dumps(tuples, 1)
        size = struct.pack('!L', len(package))
        sock.sendall(size)
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
            sys.stderr.write("Ignoring non-integer argument. Using default: %ss\n" % delay)

    sock = socket.socket()
    try:
        sock.connect( (CARBON_SERVER, CARBON_PICKLE_PORT) )
    except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is carbon-cache.py running?" % { 'server':CARBON_SERVER, 'port':CARBON_PICKLE_PORT })

    try:
        run(sock, delay)
    except KeyboardInterrupt:
        sys.stderr.write("\nExiting on CTRL-c\n")
        sys.exit(0)

if __name__ == "__main__":
    main()
