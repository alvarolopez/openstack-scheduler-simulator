# vim: tabstop=4 shiftwidth=4 softtabstop=4

import simulator
from simulator import cloud

REQUESTS = simulator.load_requests("data/trace.dat")
MAX_TIME = max([i["end"] for i in REQUESTS]) * 30

cloud.start(REQUESTS, MAX_TIME)
