import sys

from oslo.config import cfg

from nova import config
from nova import service

import simulator
from simulator import cloud


def main():
    config.parse_args(sys.argv)

    cloud.start()


if __name__ == "__main__":
    main()
