import sys

from oslo.config import cfg

from nova import config
from nova import service

#import simulator
import simulator.log as logging
from simulator import cloud

CONF = cfg.CONF



def main():
    config.parse_args(sys.argv)
    logging.setup()
    cloud.start()


if __name__ == "__main__":
    main()
