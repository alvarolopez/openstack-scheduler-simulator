import sys

from oslo.config import cfg

from nova import config
from nova import service

import simulator
from simulator import cloud

CONF = cfg.CONF


def main():
    config.parse_args(sys.argv)

    REQUESTS = simulator.load_requests("data/trace.dat")
    MAX_TIME = max([i["end"] for i in REQUESTS]) * 30

    cloud.MANAGER.setUp()
    cloud.start(REQUESTS, MAX_TIME)


if __name__ == "__main__":
    main()
