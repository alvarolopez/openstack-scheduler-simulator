import sys

from oslo.config import cfg

from nova import config
from nova import service

import simulator
from simulator import cloud


opts = [
    cfg.StrOpt('trace_file',
               metavar='TRACE_FILE',
               default='data/trace.dat',
               help=('Path to traces to process.')),
    ]

CONF = cfg.CONF
CONF.register_opts(opts, group='simulator')


def main():
    config.parse_args(sys.argv)

    cloud.start()


if __name__ == "__main__":
    main()
