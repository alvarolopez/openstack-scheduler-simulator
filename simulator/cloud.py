# vim: tabstop=4 shiftwidth=4 softtabstop=4

import simpy

from simulator import fakes
from simulator import scheduler

ENV = simpy.Environment()
JOB_STORE = simpy.Store(ENV)
MANAGER = scheduler.SchedulerManager(ENV)


def print_(who, id, what, env=ENV):
    time = env.now
    id = id[:15]
    print("%(time)2.1f %(who)10s %(id)15s %(what)s" %
          {"time": time, "who": who, "id": id, "what": what})




class Request(object):
    """A request that is scheduled at a given time.

    It creates some tasks, then requests some instances and waits until the
    tasks have finished.
    """

    def __init__(self, env, req, instance_type_name, job_store=JOB_STORE):
        self.req = req
        self.env = env
        self.instance_type_name = instance_type_name
        self.instance_type = fakes.flavors.get_flavor_by_name(instance_type_name)
        self.image = {}
        self.name = "r-%(id)s" % req
        self.jobs = []
        self.job_store = job_store

    def do(self):
        # We wait until the request is made by the user. In SimPy2 it was done
        # by activating the process at a given time using the kwarg "at=", no
        # longer present
#        print self.req["submit"], self.env.now
        yield self.env.timeout(self.req["submit"] - self.env.now)
        print_("request", self.name, "start w/ %s tasks" % self.req["tasks"])
        yield self.env.timeout(1)

        # Prepare the jobs
        # FIXME(aloga): the job store is per-instance, we should make it global
#        job_store = simpy.Store(self.env, capacity=self.req["tasks"])
        for i in xrange(self.req["tasks"]):
            jid = self.req["id"]
            wall = self.req["end"] - self.req["start"]
            job = Job(self.env, "%s-%s" % (jid, i), wall)
            self.jobs.append(job)
            self.job_store.put(job)

        # Request instances
        # Calculate how much instances I actually need. Maybe check the
        # available flavors?
        aux = divmod(self.req["tasks"], self.instance_type["cpus"])
        instance_nr = (aux[0] + 1) if aux[1] else aux[0]

        print ">>>>"
        req = scheduler.create_request_spec(self.instance_type_name, self.image, instance_nr)
#        print req
#        print instance_nr
#        print aux
        MANAGER.run_instance(req)
        print "<<<<"

        # TODO(aloga): check if we have free instances that may execute jobs
        # TODO(aloga): request instances from scheduler
#        instances = []
#        for i in xrange(instance_nr):
#            instances.append(Instance(self.env,
#                                      "ooo",
#                                      cpus=self.instance_type["cpus"],
#                                      job_store=job_store))

        for job in self.jobs:
            yield job.finished
#        yield sim.waitevent, self, job.stop_event
        print_("request", self.name, "ends")


class Instance(object):
    """An instance.

    It can run several jobs ( #jobs <= #cpus )
    """

    def __init__(self, env, name, cpus, job_store):
        self.name = name
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
        print_("instance", self.name, "starts w/ %s cpus" % self.cpus)
        yield self.env.timeout(10)
        self.process = self.env.process(self.execute())

    def shutdown(self, after=3600, soft=True):
        """Simulate the shutdown."""
        yield self.env.timeout(after)
        if soft:
            for job in self.jobs:
                yield job.finished
        self.process.interrupt()
        print_("instance", self.name, "finishes")

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

                print_("instance", self.name, "executes job %s" % job.name)
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
        print_("job", self.name, "starts (wall %s)" % self.wall)
        # Now consume the walltime
        yield self.env.timeout(self.wall)
        print_("job", self.name, "ends (wall %s)" % self.wall)
        self.finished.succeed()


class Host(object):
    """One node can run several instances."""
    def __init__(self, env, name, cpus, mem, disk):
        self.name = name
        self.cpus = cpus
        self.mem = mem
        self.disk = disk
        self.env = env
        self.instances = []
#        self.process = self.env.process(self.periodic_tasks())

    def _create_instance(self, instance_ref, job_store=JOB_STORE):
        name = instance_ref['instance_properties']['uuid']
        cpus = instance_ref['instance_type']['cpus']
        return Instance(self.env, name, cpus, job_store)

    def launch_instance(self, instance_ref):
        cpus = instance_ref['instance_type']['cpus']
        if cpus > self.cpus:
            print_("node", self.name, "cannot spawn instance ( %s < %s cpus)" %
                   (cpus, self.cpus))
            raise scheduler.exception.NoValidHost
        self.cpus -= cpus
        instance = self._create_instance(instance_ref)
        self.instances.append(instance)
        print_("node", self.name, "spawns instance %s)" % instance.name)

    def periodic_tasks(self):
        """Run forever, and check for spawned machines"""
        while True:
            instances_off = []
            for i in self.instances:
                if not i.process.is_alive:
                    instances_off.append(i)

            for i in instances_off:
                print("%2.1f %s  > node instances is off %s" % (self.env.now, self.name, i.name))
                self.cpus += i.cpus
                self.instances.remove(i)
            yield self.env.timeout(1)


def generate(env, reqs):
    """Generate the needed objects for the simulation.

    This function will generate the request objects from the traces and
    the hosts where the instances will be spawned
    """
    # FIME(aloga): really simplistic
    hosts = []
    for i in xrange(32):
        hosts.append(Host(env,
                          "node-%02d" % i,
                          8,
                          32 * 1024,
                          200 * 1024 * 1024))
    fakes.add_hosts(hosts)

    for req in reqs:
        if req["start"] < req["submit"]:
            print "discarding req %s" % req["id"]
            continue

        # FIXME(aloga). we should make this configurable. Or even adjust the
        # request to the available flavors.
        r = Request(env, req, "m1.small")
        env.process(r.do())
        yield env.timeout(0)




def start(reqs, max_time, env=ENV):
    """Start the simulation until max_time."""
    env.process(generate(env, reqs))
    # Start processes
    env.run(until=max_time)
