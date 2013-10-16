# vim: tabstop=4 shiftwidth=4 softtabstop=4

import uuid

import simpy

import simulator


class Request(object):
    """A request that is scheduled at a given time.

    It creates some tasks, then requests some instances and waits until the
    tasks have finished.
    """

    def __init__(self, env, req, instance_type):
        self.req = req
        self.env = env
        self.instance_type = simulator.INSTANCE_TYPES[instance_type]
        self.name = "r-%(id)s" % req
        self.jobs = []

    def do(self):
        # We wait until the request is made by the user. In SimPy2 it was done
        # by activating the process at a given time using the kwarg "at=", no
        # longer present
        print self.req["submit"], self.env.now
        yield self.env.timeout(self.req["submit"] - self.env.now)
        print("%2.1f %s > request starts w/ %s tasks" % (self.env.now, self.name, self.req["tasks"]))
        yield self.env.timeout(1)

        # Prepare the jobs
        # FIXME(aloga): the job store is per-instance, we should make it global
        job_store = simpy.Store(self.env, capacity=self.req["tasks"])
        for i in xrange(self.req["tasks"]):
            jid = self.req["id"]
            wall = self.req["end"] - self.req["start"]
            job = Job(self.env, "%s-%s" % (jid, i), wall)
            self.jobs.append(job)
            job_store.put(job)

        # Request instances
        # Calculate how much instances I actually need
        aux = divmod(len(job_store.items), self.instance_type["cpus"])
        instance_nr = (aux[0] + 1) if aux[0] and aux[1] else aux[0]

        # TODO(aloga): check if we have free instances that may execute jobs
        # TODO(aloga): request instances from scheduler
        instances = []
        for i in xrange(instance_nr):
            instances.append(Instance(self.env,
                                      cpus=self.instance_type["cpus"],
                                      job_store=job_store))

        for job in self.jobs:
            yield job.finished
#        yield sim.waitevent, self, job.stop_event
        print("%2.1f %s < request end" % (self.env.now, self.name))
#
#
#class Node(object):
#    """One node can run several instances."""
#    pass
#
#
class Instance(object):
    """An instance.

    It can run several jobs ( #jobs <= #cpus )
    """

    def __init__(self, env, cpus, job_store):
        self.name = uuid.uuid4().hex
        self.env = env
        self.job_store = job_store
        self.cpus = cpus
        self.jobs = []

        # Boot the machine
        self.process = self.env.process(self.boot())

        # Wait for 2 hours and shutdown
        self.env.process(self.shutdown(after=3600 * 24))

    def boot(self):
        """Simulate the boot process."""
        print("%2.1f %s > instance starts %s cpus" % (self.env.now, self.name, self.cpus))
        yield self.env.timeout(10)
        self.process = self.env.process(self.execute())

    def shutdown(self, after=3600, soft=True):
        """Simulate the shutdown."""
        yield self.env.timeout(after)
        if soft:
            for job in self.jobs:
                yield job.finished
        self.process.interrupt()
        print("%2.1f %s > instance finishes" % (self.env.now, self.name))

    def execute(self):
        """Execute all the jobs that this instance can allocate."""
        while True:
            # Get all the jobs that we can execute

            # NOTE(aloga): This code will make that the instance executes the
            # jobs in blocks (i.e. until a block has finised it will not get
            # another block.
            self.jobs = []
            jobs_to_run = min(self.cpus, max(1, len(self.job_store.items)))
            for i in xrange(jobs_to_run):
                try:
                    job = yield self.job_store.get()
                except simpy.Interrupt:
                    break

                print("%2.1f %s > instance will execute job %s" % (self.env.now, self.name, job.name))
                self.jobs.append(job)

            for job in self.jobs:
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
        """Do the job."""
        print("%2.1f %s  > job starts (wall %s)" % (self.env.now, self.name, self.wall))
        # Now consume the walltime
        yield self.env.timeout(self.wall)
        print("%2.1f %s  < job ends (wall %s)" % (self.env.now, self.name, self.wall))
        self.finished.succeed()


def generate(env, reqs):
    """Generate the request objects from the traces."""
    for req in reqs:
        if req["start"] < req["submit"]:
            print "discarding req %s" % req["id"]
            print req["start"], req["submit"]
            continue
        r = Request(env, req, "m1.small")
        env.process(r.do())
        yield env.timeout(0)


def start(reqs, max_time):
    """Start the simulation until max_time."""
    env = simpy.Environment()
    env.process(generate(env, reqs))
    # Start processes
    env.run(until=max_time)
