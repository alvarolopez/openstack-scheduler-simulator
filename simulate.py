import sys

from oslo.config import cfg

from nova import config
from nova import service

import simulator
from simulator import cloud


opts = [
    cfg.IntOpt('max_simulation_time',
               metavar='MAX_SIMULATION_TIME',
               default=0,
               help=('Maximum simulation time. If not set, the '
                     'simulation will run until all the requests '
                     'end.')),
    cfg.StrOpt('trace_file',
               metavar='TRACE_FILE',
               default='data/trace.dat',
               help=('Path to traces to process.')),
    cfg.StrOpt('default_image',
               metavar='IMAGE_UUID',
               default="3dfbc7378f244698bba512649d35d877",
               help=('Default image UUID to use if trace '
                     'file does not contain one.')),
    cfg.FloatOpt('default_image_size',
               metavar='SIZE',
               default=2.0,
               help=('Default image size to use if trace file does '
                     'not contain one.')),
    cfg.StrOpt('default_flavor',
               metavar='FLAVOR_NAME',
               default="m1.tiny",
               help=('Default flavor (instance type) to use if trace '
                     'file does not contain one.')),
    ]

CONF = cfg.CONF
CONF.register_opts(opts, group='simulator')


def main():
    config.parse_args(sys.argv)

    cloud.start()


if __name__ == "__main__":
    main()
