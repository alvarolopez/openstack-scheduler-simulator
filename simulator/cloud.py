# vim: tabstop=4 shiftwidth=4 softtabstop=4

#import datetime
#import os
import random
import uuid

import simpy

from simulator import fakes
from simulator import scheduler

ENV = simpy.Environment()
#JOB_STORE = simpy.Store(ENV, capacity=1000)
JOB_STORE = None
MANAGER = fakes.manager

#OUTDIR = os.path.join("output",
#                      datetime.datetime.now().isoformat())
#os.makedirs(OUTDIR)


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
        self.instance_type = fakes.flavors.get_flavor_by_name(
            instance_type_name)
        self.image = {"uuid": uuid.uuid4().hex,
                      "size": 0}
        self.name = "r-%(id)s" % req
        self.jobs = []
        self.job_store = job_store

    def do(self):
        # We wait until the request is made by the user. In SimPy2 it was done
        # by activating the process at a given time using the kwarg "at=", no
        # longer present
        yield self.env.timeout(self.req["submit"] - self.env.now)
        start = self.env.now
        print_("request", self.name, "start w/ %s tasks" % self.req["tasks"])
        yield self.env.timeout(1)

        # Prepare the jobs
        jid = self.req["id"]
        wall = self.req["end"] - self.req["start"]
        expected_elapsed = self.req["end"] - self.req["submit"]
        for i in xrange(self.req["tasks"]):
            job = Job(self.env, "%s-%s" % (jid, i), wall)
            self.jobs.append(job)
            self.job_store.put(job)

        # Request instances
        # Calculate how much instances I actually need. Maybe check the
        # available flavors?
        aux = divmod(self.req["tasks"], self.instance_type["cpus"])
        instance_nr = (aux[0] + 1) if aux[1] else aux[0]

        # Request the instance_nr that we need
        req = fakes.create_request_spec(self.instance_type_name,
                                        self.image,
                                        instance_nr)
        MANAGER.run_instance(req, self.job_store)

        instance_uuids = req["instance_uuids"]

        print_("request", self.name, "got instances: %s" % instance_uuids)

        for job in self.jobs:
            yield job.finished

        end = self.env.now
        for instance_uuid in instance_uuids:
            pass
            MANAGER.terminate_instance(instance_uuid)

        msg = ("ends. expected wall %s, "
               "expected elapsed %s, "
               "elapsed %s" % (wall, expected_elapsed, end - start))
        print_("request", self.name, msg)


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

        self.boot_time = round(random.uniform(10, 20))

        self.node_resources = node_resources

        self.finished = self.env.event()

        # Boot the machine
        self.process = self.env.process(self.boot())

        # Wait for 2 hours and shutdown
#        self.env.process(self.shutdown(after=3600 * 24))

    def boot(self):
        """Simulate the boot process.

        Before actually booting the image, we will consume resources from
        the host.
        """
        # Consume the node_resources 1st. Do not check if they're available,
        # since check was made upstream
        for resource in ("cpus", "mem", "disk"):
            amount = getattr(self, resource, 0)
            if amount == 0:
                continue
            self.node_resources[resource].get(amount)
            print_("instance",
                   self.name,
                   "consumes %s %s" % (amount, resource))

        # Spawn
        print_("instance", self.name, "starts w/ %s cpus" % self.cpus)
        yield self.env.timeout(self.boot_time)
        self.process = self.env.process(self.execute())

    def shutdown(self, after=0):
        """Simulate the shutdown.

        After shutting the instance down, free the allocated resources.
        """
        yield self.env.timeout(after)
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
            jobs = []

            # Start executing jobs
            # If we want to execute jobs in blocs (instead of start consuming)
            # we can get the number of jobs to get with the following
#            jobs_to_run = min(self.cpus, max(1, len(self.job_store.items)))
            for job in xrange(self.cpus):
                with self.job_store.get() as req:
                    try:
                        result = yield req | self.env.timeout(1)
                    except simpy.Interrupt:
                        return

                    if req in result:
                        job = result[req]
                        jobs.append(job)
                    else:
                        break

            for job in jobs:
                # Start the jobs
                job.start()

            for job in jobs:
                # Wait until all jobs are finished
                try:
                    yield job.finished
                except simpy.Interrupt:
                    return


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

        self.images = {}

    def _download(self, image_uuid):
        """Download an image to disk."""
        print_("host", self.name, "download %s starts" % image_uuid)
        yield self.env.timeout(0)  # FIXME(aloga): model this
        print_("host", self.name, "download %s ends" % image_uuid)
        self.images[image_uuid]["status"] = "DOWNLOADED"
        self.images[image_uuid]["downloaded"].succeed()

    def _duplicate(self, image):
        """Copy the image so that we can use it."""
        yield self.env.timeout(0)  # FIXME(aloga): model this

    def _resize(self, image):
        """Resize the image to the actual size."""
        yield self.env.timeout(0)  # FIXME(aloga): model this

    def _prepare_image(self, instance_ref):
        """Prepare the image before we spawn the instance.

        This method will either download the image or wait until it has been
        downloaded (if there's another download in progress). Then it will
        duplicate and resize the image.
        """
        image = instance_ref["image"]
        image_uuid = image["uuid"]
        self.images.setdefault(image_uuid,
                               {"status": None,
                                "downloaded": self.env.event()})
        status = self.images[image_uuid]["status"]
        if status not in ("DOWNLOADED", "DOWNLOADING"):
            self.images[image_uuid]["status"] = "DOWNLOADING"
            yield self.env.process(self._download(image_uuid))
        elif status == "DOWNLOADING":
            yield self.images[image_uuid]["downloaded"]
        yield self.env.process(self._duplicate(image))
        yield self.env.process(self._resize(image))

    def _create_instance(self, instance_uuid, instance_ref, job_store):
        """Actually create the image."""
        yield self.env.process(self._prepare_image(instance_ref))
        instance_type = instance_ref['instance_type']
        instance = Instance(self.env,
                            instance_uuid,
                            instance_type,
                            job_store,
                            self.resources)
        self.instances[instance_uuid] = instance

        MANAGER.change_status(instance_uuid, "ACTIVE")
        print_("node", self.name, "spawns instance %s" % instance.name)

    def terminate_instance(self, instance_uuid):
        """Terminate the instance."""
        print_("host", self.name, "terminates %s" % instance_uuid)
        instance = self.instances.pop(instance_uuid)
        self.env.process(instance.shutdown())

    def launch_instance(self, instance_uuid,
                        instance_ref, job_store=JOB_STORE):
        """Launch an instance if we can allocate it.

        We will raise a scheduler.exception.NoValidHost if we cannot
        allocate resource for the image. FIXME(aloga): we should raise
        nova.exception.ComputeResourcesUnavailable ?
        """
        for i in ('cpus', 'mem', 'disk'):
            res = instance_ref['instance_type'][i]
            if res > self.resources[i].level:
                msg = ("cannot spawn instance ( %s > %s %s)" %
                       (res, self.resources[i].level, i))
                print_("node", self.name, msg)
                raise scheduler.exception.NoValidHost(reason=msg)

        self.env.process(self._create_instance(instance_uuid,
                                               instance_ref,
                                               job_store))


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
        job_store = simpy.Store(ENV, capacity=1000)
        r = Request(env, req, "m1.small", job_store=job_store)
        env.process(r.do())
        yield env.timeout(0)


def start(reqs, max_time, env=ENV):
    """Start the simulation until max_time."""
    env.process(generate(env, reqs))
    # Start processes
    env.run(until=max_time)
