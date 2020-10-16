#!/usr/bin/python3

import sys

if __name__ == "__main__":

    hostBlocks = {}

    for line in sys.stdin:
        if "hostPort" in line:
            host = line.split('"')[3].split(':')[0]
            if not host in hostBlocks:
                hostBlocks[host] = 0
            lasthost = host
        elif "rddBlocks" in line:
            blocks = int(line.split(':')[1].replace(',', '').strip())
            hostBlocks[lasthost] = hostBlocks[lasthost] + blocks

    target = max(hostBlocks, key=hostBlocks.get)

    print(target)
