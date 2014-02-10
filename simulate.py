import sys

from oslo.config import cfg

from nova import config
from nova import service

#import simulator
import simulator.log as logging

config.parse_args(sys.argv)
logging.setup()


def main():
    from simulator import cloud
    cloud.start()


if __name__ == "__main__":
    main()
