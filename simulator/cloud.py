# vim: tabstop=4 shiftwidth=4 softtabstop=4

import random
import uuid

import simpy

from oslo.config import cfg

from nova import exception

import simulator
import simulator.catalog
from simulator import scheduler
from simulator import utils

CONF = cfg.CONF

ENV = simulator.ENV
MANAGER = simulator.scheduler.manager
CATALOG = simulator.catalog.CATALOG


class Request(object):
    """A request that is scheduled at a given time.

    It creates some tasks, then requests some instances and waits until the
    tasks have finished.
    """

    def __init__(self, req, instance_type_name, job_store):
        self.req = req

        self.instance_type_name = instance_type_name
        self.instance_type = simulator.scheduler.flavors.get_flavor_by_name(
            instance_type_name)
        # FIXME(aloga): this should be stored in the catalog
        self.image = {"uuid": uuid.uuid4().hex,
                      "size": 10}
        self.name = "r-%(id)s" % req
        self.jobs = []
        self.job_store = job_store

    def do(self):
        # We wait until the request is made by the user. In SimPy2 it was done
        # by activating the process at a given time using the kwarg "at=", no
        # longer present
        yield ENV.timeout(self.req["submit"] - ENV.now)
        start = ENV.now
        utils.print_("request",
                     self.name,
                     "start w/ %s tasks" % self.req["tasks"])
        yield ENV.timeout(1)

        # Prepare the jobs
        jid = self.req["id"]
        wall = self.req["end"] - self.req["start"]
        expected_elapsed = self.req["end"] - self.req["submit"]
        for i in xrange(self.req["tasks"]):
            job = Job("%s-%s" % (jid, i), wall)
            self.jobs.append(job)
            self.job_store.put(job)

        # Request instances
        # Calculate how much instances I actually need. Maybe check the
        # available flavors?
        aux = divmod(self.req["tasks"], self.instance_type["cpus"])
        instance_nr = (aux[0] + 1) if aux[1] else aux[0]

        # Request the instance_nr that we need
        req = simulator.scheduler.create_request_spec(
            self.instance_type_name,
            self.image,
            instance_nr)
        MANAGER.run_instance(req, self.job_store)

        instance_uuids = req["instance_uuids"]

        utils.print_("request",
                     self.name,
                     "got instances: %s" % instance_uuids)

        for job in self.jobs:
            yield job.finished

        end = ENV.now
        for instance_uuid in instance_uuids:
            pass
            MANAGER.terminate_instance(instance_uuid)

        msg = ("ends. expected wall %s, "
               "expected elapsed %s, "
               "elapsed %s" % (wall, expected_elapsed, end - start))
        utils.print_("request", self.name, msg)


class Instance(object):
    """An instance.

    It can run several jobs ( #jobs <= #cpus )
    """

    def __init__(self, name, instance_type, job_store, node_resources):
        self.name = name

        self.job_store = job_store
        self.cpus = instance_type["cpus"]
        self.mem = instance_type["mem"]
        self.disk = instance_type["disk"]

        self.boot_time = round(random.uniform(10, 20))

        self.node_resources = node_resources

        self.finished = ENV.event()

        # Boot the machine
        self.process = ENV.process(self.boot())

        # Wait for 2 hours and shutdown
#        ENV.process(self.shutdown(after=3600 * 24))

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
            utils.print_("instance",
                         self.name,
                         "consumes %s %s" % (amount, resource))

        # Spawn
        utils.print_("instance", self.name, "starts w/ %s cpus" % self.cpus)
        yield ENV.timeout(self.boot_time)
        self.process = ENV.process(self.execute())

    def shutdown(self, after=0):
        """Simulate the shutdown.

        After shutting the instance down, free the allocated resources.
        """
        yield ENV.timeout(after)
        self.process.interrupt()
        utils.print_("instance", self.name, "finishes")

        # Free the node_resources
        for resource in ("cpus", "mem", "disk"):
            amount = getattr(self, resource, 0)
            if amount == 0:
                continue
            self.node_resources[resource].put(amount)
            utils.print_("instance",
                         self.name,
                         "frees %s %s" % (amount, resource))

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
                        result = yield req | ENV.timeout(1)
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
    def __init__(self, jid, wall):
        self.name = "j-%s" % jid

        self.wall = wall
        self.finished = ENV.event()

    def start(self):
        ENV.process(self.do())

    def do(self):
        """Do the job."""
        utils.print_("job", self.name, "starts (wall %s)" % self.wall)
        # Now consume the walltime
        yield ENV.timeout(self.wall)
        utils.print_("job", self.name, "ends (wall %s)" % self.wall)
        self.finished.succeed()


class Host(object):
    """One node can run several instances."""
    def __init__(self, name, cpus, mem, disk):
        self.name = name
        self.cpus = cpus
        self.mem = mem
        self.disk = disk

        self.instances = {}
        self.resources = {
            "cpus": simpy.Container(ENV, self.cpus, init=self.cpus),
            "mem": simpy.Container(ENV, self.mem, init=self.mem),
            "disk": simpy.Container(ENV, self.disk, init=self.disk),
        }

        self.disk_bw = 0.2

        self.images = {}

    def _download(self, image):
        """Download an image to disk."""
        image_uuid = image["uuid"]

        utils.print_("host", self.name, "download of %s starts" % image_uuid)
        # Download the image from the catalog
        yield ENV.process(CATALOG.download(image))
        utils.print_("host", self.name, "download of %s ends" % image_uuid)
        self.images[image_uuid]["status"] = "DOWNLOADED"
        self.images[image_uuid]["downloaded"].succeed()

    def _duplicate(self, image):
        """Copy the image so that we can use it."""
        variation = random.uniform(0.9, 1)
        copy_time = variation * image["size"] / self.disk_bw
        yield ENV.timeout(copy_time)

    def _resize(self, image):
        """Resize the image to the actual size."""
        variation = random.uniform(0.9, 1)
        resize_time = variation * 0
        # FIXME(aloga): we need to model this too
        yield ENV.timeout(resize_time)

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
                                "downloaded": ENV.event()})
        status = self.images[image_uuid]["status"]
        if status not in ("DOWNLOADED", "DOWNLOADING"):
            self.images[image_uuid]["status"] = "DOWNLOADING"
            yield ENV.process(self._download(image))
        elif status == "DOWNLOADING":
            yield self.images[image_uuid]["downloaded"]
        yield ENV.process(self._duplicate(image))
        yield ENV.process(self._resize(image))

    def _create_instance(self, instance_uuid, instance_ref, job_store):
        """Actually create the image."""
        yield ENV.process(self._prepare_image(instance_ref))
        instance_type = instance_ref['instance_type']
        instance = Instance(instance_uuid,
                            instance_type,
                            job_store,
                            self.resources)
        self.instances[instance_uuid] = instance

        MANAGER.change_status(instance_uuid, "ACTIVE")
        utils.print_("node", self.name, "spawns instance %s" % instance.name)

    def terminate_instance(self, instance_uuid):
        """Terminate the instance."""
        utils.print_("host", self.name, "terminates %s" % instance_uuid)
        instance = self.instances.pop(instance_uuid)
        ENV.process(instance.shutdown())

    def launch_instance(self, instance_uuid,
                        instance_ref, job_store):
        """Launch an instance if we can allocate it.

        We will raise a exception.NoValidHost if we cannot
        allocate resource for the image. FIXME(aloga): we should raise
        nova.exception.ComputeResourcesUnavailable ?
        """
        for i in ('cpus', 'mem', 'disk'):
            res = instance_ref['instance_type'][i]
            if res > self.resources[i].level:
                msg = ("cannot spawn instance ( %s > %s %s)" %
                       (res, self.resources[i].level, i))
                utils.print_("node", self.name, msg)
                raise exception.NoValidHost(reason=msg)

        ENV.process(self._create_instance(instance_uuid,
                                          instance_ref,
                                          job_store))


def generate(reqs):
    """Generate the needed objects for the simulation.

    This function will generate the request objects from the traces and
    the hosts where the instances will be spawned
    """

    # FIME(aloga): really simplistic
    hosts = []
    for i in xrange(32):
        hosts.append(Host("node-%02d" % i,
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
        r = Request(req, "m1.small", job_store)
        ENV.process(r.do())
        yield ENV.timeout(0)


def start():
    """Start the simulation until max_time."""

    reqs = utils.load_requests(CONF.simulator.trace_file)
    if CONF.simulator.max_simulation_time:
        max_time = CONF.simulator.max_simulation_time
    else:
        max_time = max([i["end"] for i in reqs]) * 30

    MANAGER.setUp()
    ENV.process(generate(reqs))
    # Start processes
    ENV.run(until=max_time)
