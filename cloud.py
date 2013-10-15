import math
import uuid

import simpy

## Model components -----------------------------


class Request(object):
    """A request"""
    def __init__(self, env, req, instance_type):
        self.req = req
        self.env = env
        self.instance_type = INSTANCE_TYPES[instance_type]
        self.name = "r-%(id)s" % req
        self.jobs = []

    def do(self):
        print self.req["submit"], self.env.now
        yield self.env.timeout(self.req["submit"] - self.env.now)
        print("%2.1f %s > request starts w/ %s tasks" % (env.now, self.name, self.req["tasks"]))
        yield self.env.timeout(1)
        # TODO: 1.- request nodes, spawn instances, run jobs
        # Run the job

        # Create all the jobs that will be consumed by the instances
        # FIXME(aloga): the job store is per-instance, we should make it global
        job_store = simpy.Store(env, capacity=self.req["tasks"])
        for i in xrange(self.req["tasks"]):
            jid = self.req["id"]
            wall = self.req["end"] - self.req["start"]
            job = Job(self.env, "%s-%s" % (jid, i), wall)
            self.jobs.append(job)
            job_store.put(job)

        # Calculate how much instances I actually need
        aux = divmod(len(job_store.items), self.instance_type["cpus"])
        instance_nr = (aux[0] + 1) if aux[0] and aux[1] else aux[0]

        # Request the nr instances. This should request N nodes that will
        # accomodate the instances. Then the instances will be spawned in each
        # of the nodes.
        instances = []
        for i in xrange(instance_nr):
            instances.append(Instance(env,
                                      cpus=self.instance_type["cpus"],
                                      job_store=job_store))

        for job in self.jobs:
            yield job.finished
#        yield sim.waitevent, self, job.stop_event
        print("%2.1f %s < request end" % (env.now, self.name))
#
#
#class Node(object):
#    """One node can run several instances."""
#    pass
#
#
class Instance(object):
    """One instance can run several jobs."""
    def __init__(self, env, cpus, job_store):
        self.name = uuid.uuid4().hex
        self.env = env
        self.job_store = job_store
        self.cpus = cpus
        self.env.process(self.execute())
#        sim.activate(self, self.execute(), at=env.now())

    def execute(self):
        """Execute all the jobs that this instance can allocate."""

        print("%2.1f %s > instance starts %s cpus" % (env.now, self.name, self.cpus))

        yield self.env.timeout(10)
#        def get_job(buffer):
#            result = []
#            for i in enumerate(buffer):
#                if i[0] < self.cpus:
#                    result.append(i[1])
#            return result
#
        while True:
            # Get all the jobs that we can execute

            # NOTE(aloga): This code will make that the instance executes the
            # jobs in blocks (i.e. until a block has finised it will not get
            # another block.
            jobs_to_run = min(self.cpus, max(1, len(self.job_store.items)))
            jobs = []
            for i in xrange(jobs_to_run):
                job = yield self.job_store.get()
                print("%2.1f %s > instance will execute job %s" % (env.now, self.name, job.name))
                jobs.append(job)

            for job in jobs:
                # Wait until all jobs are finished
                yield self.env.process(job.do())


class Job(object):
    """One Job executed inside an instance."""
    def __init__(self, env, jid, wall):
        self.name = "j-%s" % jid
        self.env = env
        self.wall = wall
        self.finished = self.env.event()

    def do(self):
        print("%2.1f %s  > job starts (wall %s)" % (env.now, self.name, self.wall))
        # Now consume the walltime
        yield self.env.timeout(self.wall)
        print("%2.1f %s  < job ends (wall %s)" % (self.env.now, self.name, self.wall))
        self.finished.succeed()


### Experiment data ------------------------------

reqs = []
with open("data/trace.dat") as f:
    for req in f.readlines():
        req = req.strip()
        if req.startswith("#"):
            continue
        req = req.split()
        reqs.append({"id": req[0],
                     "owner": req[1],
                     "submit": float(req[2]),
                     "start": float(req[3]),
                     "end": float(req[4]),
                     "tasks": int(req[5]),
        })

maxTime = max([i["end"] for i in reqs])


### Model/Experiment ------------------------------

INSTANCE_TYPES = {
    "m1.small": {"cpus": 4},
}

def generate(env, reqs):
    for req in reqs:
        if req["start"] < req["submit"]:
            print "discarding req %s" % req["id"]
            print req["start"], req["submit"]
            continue
        r = Request(env, req, "m1.small")
        env.process(r.do())
        yield env.timeout(0)


env = simpy.Environment()
env.process(generate(env, reqs))
# Start processes
env.run(until=maxTime*3)
