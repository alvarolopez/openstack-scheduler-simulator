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
#               help=_('Path to simulation definition')),
               help=('Path to traces to process.')),
    ]

CONF = cfg.CONF
CONF.register_opts(opts, group='simulator')

def main():
    config.parse_args(sys.argv)

    REQUESTS = simulator.load_requests(CONF.simulator.trace_file)
    MAX_TIME = max([i["end"] for i in REQUESTS]) * 30

    cloud.MANAGER.setUp()
    cloud.start(REQUESTS, MAX_TIME)


if __name__ == "__main__":
    main()
