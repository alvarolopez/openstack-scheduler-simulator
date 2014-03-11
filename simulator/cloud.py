# vim: tabstop=4 shiftwidth=4 softtabstop=4

import os
import os.path

import simpy

from oslo.config import cfg

import simulator
import simulator.hosts
import simulator.log as logging
import simulator.requests
import simulator.scheduler
import simulator.utils

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
                 help=('Default image size (GB) to use if trace file does '
                       'not contain one.')),
    cfg.StrOpt('default_flavor',
               metavar='FLAVOR_NAME',
               default="m1.tiny",
               help=('Default flavor (instance type) to use if trace '
                     'file does not contain one.')),
    cfg.StrOpt('output_dir',
               metavar='DIR',
               default='outputs',
               help='Where to store simulation output files'),
]

CONF = cfg.CONF
CONF.register_opts(opts, group='simulator')

LOG = logging.getLogger(__name__)

ENV = simulator.ENV
MANAGER = simulator.scheduler.manager


def generate(reqs):
    """Generate the needed objects for the simulation.

    This function will generate the request objects from the traces and
    the hosts where the instances will be spawned
    """

    # FIME(aloga): really simplistic
    hosts = []
    for i in xrange(1):
        hosts.append(simulator.hosts.Host("node-%02d" % i,
                                          2,
                                          32 * 1024,
                                          200 * 1024 * 1024))
    MANAGER.add_hosts(hosts)

    for req in reqs:
#        if req["start"] != 0 and req["start"] < req["submit"]:
#            print "discarding req %s" % req["id"]
#            continue

        # FIXME(aloga). we should make this configurable. Or even adjust the
        # request to the available flavors.
        job_store = simpy.Store(ENV, capacity=1000)
        r = simulator.requests.Request(req, job_store)
        ENV.process(r.do())
        yield ENV.timeout(0)


def start():
    """Start the simulation until max_time."""

    if not os.path.exists(CONF.simulator.output_dir):
        os.mkdir(CONF.simulator.output_dir)

    reqs = simulator.utils.load_requests(CONF.simulator.trace_file)
    if CONF.simulator.max_simulation_time is not None:
        max_time = CONF.simulator.max_simulation_time
    else:
        max_time = max([i["end"] for i in reqs]) * 30

    MANAGER.setUp()
    ENV.process(generate(reqs))
    # Start processes
    if max_time != 0:
        ENV.run(until=max_time)
    else:
        ENV.run()
