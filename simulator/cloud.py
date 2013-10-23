# vim: tabstop=4 shiftwidth=4 softtabstop=4

import simpy

from simulator import fakes
from simulator import scheduler

ENV = simpy.Environment()
JOB_STORE = simpy.Store(ENV, capacity=1000)
MANAGER = fakes.manager


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

        # Request the instance_nr that we need
        req = fakes.create_request_spec(self.instance_type_name, self.image, instance_nr)
        MANAGER.run_instances(req)

        uuids = req["instance_uuids"]

        print_("request", self.name, "got the following instances: %s" % uuids)

        for job in self.jobs:
            yield job.finished

        for u in uuids:
            MANAGER.terminate_instance(u)

        print_("request", self.name, "ends")


class Instance(object):
    """An instance.

    It can run several jobs ( #jobs <= #cpus )
    """

    def __init__(self, env, name, instance_type, job_store, node_resources):
        self.name = name
        self.env = env
        self.job_store = job_store
        self.cpus = instance_type["cpus"]
        self.mem = instance_type["mem"]
        self.disk = instance_type["disk"]
        self.jobs = []

        self.boot_time = 10  # seconds

        self.node_resources = node_resources

        self.finished = self.env.event()

        # Boot the machine
        self.process = self.env.process(self.boot())

        # Wait for 2 hours and shutdown
#        self.env.process(self.shutdown(after=3600 * 24))


    def boot(self):
        """Simulate the boot process."""
        # Consume the node_resources 1st. Do not check if they're available,
        # since check was made upstream
        for resource in ("cpus", "mem", "disk"):
            amount = getattr(self, resource, 0)
            if amount == 0:
                continue
            self.node_resources[resource].get(amount)
            print_("instance", self.name, "consumes %s %s" % (amount, resource))

        # Spawn
        print_("instance", self.name, "starts w/ %s cpus" % self.cpus)
        yield self.env.timeout(self.boot_time)
        self.process = self.env.process(self.execute())

    def shutdown(self, after=3600, soft=True):
        """Simulate the shutdown."""
        yield self.env.timeout(after)
        if soft:
            for job in self.jobs:
                yield job.finished
        self.process.interrupt()
        print_("instance", self.name, "finishes")

        # Free the node_resources
        for resource in ("cpus", "mem", "disk"):
            amount = getattr(self, resource, 0)
            if amount == 0:
                continue
            self.node_resources[resource].put(amount)
            print_("instance", self.name, "frees %s %s" % (amount, resource))

        self.finished.succeed()

    def execute(self):
        """Execute all the jobs that this instance can allocate."""
        while True:

            # NOTE(aloga): This code will make that the instance executes the
            # jobs in blocks (i.e. until a block has finised it will not get
            # another block.
            self.jobs = []
            jobs_to_run = min(self.cpus, max(1, len(self.job_store.items)))
            for i in xrange(jobs_to_run):
                # Get all the jobs that we can execute
                try:
                    job = yield self.job_store.get()
                except simpy.Interrupt:
                    return

                print_("instance", self.name, "executes job %s" % job.name)
                self.jobs.append(job)

            for job in self.jobs:
                # Start the jobs
                job.start()

            for job in self.jobs:
                # Wait until all jobs are finished
                yield job.finished


class Job(object):
    """One Job executed inside an instance."""
    def __init__(self, env, jid, wall):
        self.name = "j-%s" % jid
        self.env = env
        self.wall = wall
        self.finished = self.env.event()

    def start(self):
        self.env.process(self.do())

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
        self.instances = {}
        self.resources = {
            "cpus": simpy.Container(self.env, self.cpus, init=self.cpus),
            "mem": simpy.Container(self.env, self.mem, init=self.mem),
            "disk": simpy.Container(self.env, self.disk, init=self.disk),
        }

    def _create_instance(self, instance_ref, job_store=JOB_STORE):
        name = instance_ref['instance_properties']['uuid']
        instance_type =  instance_ref['instance_type']
        return Instance(self.env, name, instance_type, job_store, self.resources)

    def terminate_instance(self, uuid):
        instance = self.instances.pop(uuid)
        self.env.process(instance.shutdown())

    def launch_instance(self, instance_ref):
        for i in ('cpus', 'mem', 'disk'):
            res = instance_ref['instance_type'][i]
            if res > self.resources[i].level:
                msg = ("cannot spawn instance ( %s > %s %s)" %
                       (res, self.resources[i].level, i))
                print_("node", self.name, msg)
                raise scheduler.exception.NoValidHost(reason=msg)

        instance = self._create_instance(instance_ref)
        uuid = instance_ref["instance_properties"]["uuid"]
        self.instances[uuid] = instance
        print_("node", self.name, "spawns instance %s" % instance.name)


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
                          4,
                          32 * 1024,
                          200 * 1024 * 1024))
    MANAGER.add_hosts(hosts)

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
