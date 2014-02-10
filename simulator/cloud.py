# vim: tabstop=4 shiftwidth=4 softtabstop=4

import datetime
import random
import os
import os.path

import simpy

from oslo.config import cfg

from nova import exception

import simulator
import simulator.catalog
import simulator.scheduler
from simulator import utils

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

ENV = simulator.ENV
MANAGER = simulator.scheduler.manager
CATALOG = simulator.catalog.CATALOG


class Request(object):
    """A request that is scheduled at a given time.

    A request will try to get as many instances as needed to satisfy the
    cores needed by the user, then wait until the job has finished.
    """

    def __init__(self, req, job_store):
        self.req = req

        self.flavor_name = req["flavor"] or CONF.simulator.default_flavor
        self.flavor = simulator.scheduler.flavors.get_flavor_by_name(
            self.flavor_name)
        # FIXME(aloga): this should be stored in the catalog

        self.image = {
            "uuid": req["image"] or CONF.simulator.default_image,
            "size": req["size"] or CONF.simulator.default_image_size,
        }
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
                     "start w/ %s cores" % self.req["cores"])
        yield ENV.timeout(1)

        # Prepare the jobs
        jid = self.req["id"]
        wall = self.req["end"] - self.req["start"]
        expected_elapsed = self.req["end"] - self.req["submit"]
        if wall > 0:
            for i in xrange(self.req["cores"]):
                job = Job("%s-%s" % (jid, i), wall)
                self.jobs.append(job)
                self.job_store.put(job)

        # Request instances
        # Calculate how much instances I actually need. Maybe check the
        # available flavors?
        aux = divmod(self.req["cores"], self.flavor["vcpus"])
        instance_nr = (aux[0] + 1) if aux[1] else aux[0]

        # Request the instance_nr that we need
        req_spec = simulator.scheduler.create_request_spec(
            self.req["id"],
            self.flavor_name,
            self.image,
            instance_nr)

        instance_uuids = req_spec["instance_uuids"]

        MANAGER.run_instance(req_spec, self.job_store)

        instance_uuids = req_spec["instance_uuids"]

        utils.print_("request",
                     self.name,
                     "got instances: %s" % instance_uuids)

#        for job in self.jobs:
#            yield job.finished


        if self.req["terminate"]:
            wait_until = self.req["terminate"]
            timeout = ENV.timeout(wait_until)
        else:
            timeout = None

        events = [j.finished for j in self.jobs]

        # Wait until all jobs have finished or until the request is terminated
        # or until simulator ends
        if timeout and events:
            what = yield timeout | ENV.all_of(events)
        elif events:
            what = yield ENV.all_of(events)
        elif timeout:
            what = yield timeout
        else:
            while True:
                timeout = ENV.timeout(1)
                what = yield timeout
                statuses = []
                for instance_uuid in instance_uuids:
                    status = MANAGER.get_instance_status(instance_uuid)
                    statuses.append(status)
                if all([i == "ACTIVE" for i in statuses]):
                    break

            instances = MANAGER.get_instances_from_req(self.req["id"])
            what = yield ENV.all_of([i["instance"].finished for i in instances])

        if what and timeout in what:
            # The request is terminated, do we have jobs waiting?
            msg = "terminated before the jobs completed."
        elif events:
            # Jobs are terminated, wait until request is terminated
            yield timeout
            end = ENV.now
            msg = ("terminated and jobs completed. expected wall %s, "
                   "expected elapsed %s, "
                   "elapsed %s" % (wall, expected_elapsed, end - start))
        else:
            msg = "terminated."

        for instance_uuid in instance_uuids:
            MANAGER.terminate_instance(instance_uuid)
        utils.print_("request", self.name, msg)


class Instance(object):
    """An instance.

    It can run several jobs ( #jobs <= #vcpus )
    """

    def __init__(self, name, instance_type, job_store, node_resources):
        self.name = name

        self.job_store = job_store
        self.vcpus = instance_type["vcpus"]
        self.memory_mb = instance_type["memory_mb"]
        self.root_gb = instance_type["root_gb"]
        self.ephemeral_gb = instance_type["ephemeral_gb"]

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
        for resource in ("vcpus", "memory_mb", "root_gb", "ephemeral_gb"):
            amount = getattr(self, resource, 0)
            if amount == 0:
                continue
            if resource in ("root_gb", "ephemeral_gb"):
                resource = "disk"

            with self.node_resources[resource].get(amount) as request:
                result = yield request | ENV.timeout(0)
                if request not in result:
                    # FIXME(aloga): we need to capture this
                    raise exception.ComputeResourcesUnavailable()

            utils.print_("instance",
                         self.name,
                         "consumes %s (%s left) %s" %
                            (amount,
                             self.node_resources[resource].level,
                             resource))

        # Spawn
        utils.print_("instance", self.name, "starts w/ %s vcpus" % self.vcpus)
        try:
            yield ENV.timeout(self.boot_time)
        except simpy.Interrupt:
            return
        self.process = ENV.process(self.execute())

    def shutdown(self, after=0):
        """Simulate the shutdown.

        After shutting the instance down, free the allocated resources.
        """
        yield ENV.timeout(after)
        self.process.interrupt()
        utils.print_("instance", self.name, "finishes")

        # Free the node_resources
        # FIXME(DRY)
        for resource in ("vcpus", "memory_mb", "root_gb", "ephemeral_gb"):
            amount = getattr(self, resource, 0)
            if amount == 0:
                continue

            if resource in ("root_gb", "ephemeral_gb"):
                resource = "disk"

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
            for job in xrange(self.vcpus):
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
        if self.wall !=0:
            ENV.process(self.do())

    def do(self):
        """Do the job."""
        utils.print_("job", self.name, "starts (wall %s)" % self.wall)
        # Now consume the walltime
        yield ENV.timeout(self.wall)
        utils.print_("job", self.name, "ends (wall %s)" % self.wall)
        self.finished.succeed()


class Host(dict):
    """One node can run several instances."""
    def __init__(self, name, vcpus, memory_mb, disk):
        self.name = name
        self.vcpus = vcpus
        self.memory_mb = memory_mb
        self.disk = disk
        self.total_disk = disk

        self.instances = {}
        self.resources = {
            "vcpus": simpy.Container(ENV, self.vcpus, init=self.vcpus),
            "memory_mb": simpy.Container(ENV, self.memory_mb, init=self.memory_mb),
            "disk": simpy.Container(ENV, self.disk, init=self.disk),
        }

        self.disk_bw = 0.2

        self.images = {}

        self.created_at = datetime.datetime.utcnow()

        self.service = {
            'updated_at': self.updated_at,
            'created_at': self.created_at,
            'host': self.name,
            'disabled': False
        }

        self.instance_uuids = []
        self.instance_tasks = {}
        self.disk_available_least = None

    def __str__(self):
        return "Host %s" % self.name

    @property
    def updated_at(self):
        return datetime.datetime.utcnow()

    def get_host_info(self):
        d = {'free_disk_gb': self.resources["disk"].level,
             'local_gb_used': self.resources["disk"].capacity - self.resources["disk"].level,
             'local_gb': self.resources["disk"].capacity,
             'free_ram_mb': self.resources["memory_mb"].level,
             'memory_mb': self.resources['memory_mb'].capacity,
             'vcpus': self.resources['vcpus'].capacity,
             'vcpus_used': self.resources['vcpus'].capacity - self.resources['vcpus'].level,
             'updated_at': self.updated_at,
             'created_at': self.created_at,
             'hypervisor_hostname': self.name,
             'hypervisor_type': None,
             'hypervisor_version': None,
             'disk_available_least': None,
             'host_ip': None,
             'cpu_info': None,
             'supported_instances': None,
             'name': self.name,
             'service': self.service,
             'instance_uuids': self.instance_uuids,
        }
        return d

    def __getitem__(self, k):
        return self.get_host_info()[k]

    def __iter__(self):
        return self.get_host_info().keys()

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
        if CONF.use_cow_images:
            # FIXME(aloga): model this
            copy_time = 1
        else:
            variation = random.uniform(0.9, 1)
            copy_time = image["size"] / (variation * self.disk_bw)

        yield ENV.timeout(copy_time)

    def _resize(self, image, root, ephemeral):
        """Resize the image to the actual size."""
        if CONF.use_cow_images:
            resize_time = 1
        else:
            variation = random.uniform(0.9, 1)
            resize_time = variation * 0
        # FIXME(aloga): we need to actually resize the filesystems
        yield ENV.timeout(resize_time)

    def _prepare_image(self, instance_ref):
        """Prepare the image before we spawn the instance.

        This method will either download the image or wait until it has been
        downloaded (if there's another download in progress). Then it will
        duplicate and resize the image.
        """
        instance_uuid = instance_ref["instance_properties"]["uuid"]
        image = instance_ref["image"]
        image_uuid = image["uuid"]

        # Check if the image is being downloaded, is downloaded or
        # we need to download it.
        self.images.setdefault(image_uuid,
                               {"status": None,
                                "downloaded": ENV.event()})
        status = self.images[image_uuid]["status"]
        if status not in ("DOWNLOADED", "DOWNLOADING"):
            # We need to download it
            self.images[image_uuid]["status"] = "DOWNLOADING"
            self.instance_tasks[instance_uuid] = ENV.process(self._download(image))
            try:
                yield self.instance_tasks[instance_uuid]
            except simpy.Interrupt:
                self.images.pop(image_uuid, None)
                raise
        elif status == "DOWNLOADING":
            # It is beign downloaded, wait for it
            self.instance_tasks[instance_uuid] = self.images[image_uuid]["downloaded"]
            yield self.instance_tasks[instance_uuid]

        # Next, copy the image
        self.instance_tasks[instance_uuid] = ENV.process(self._duplicate(image))
        yield self.instance_tasks[instance_uuid]

        root = instance_ref["instance_properties"]["root_gb"]
        ephemeral = instance_ref["instance_properties"]["ephemeral_gb"]

        # Next, resize the image
        self.instance_tasks[instance_uuid] = ENV.process(self._resize(image, root, ephemeral))
        yield self.instance_tasks[instance_uuid]



    def _create_instance(self, instance_uuid, instance_ref, job_store):
        """Actually create the image."""
        self.instance_tasks[instance_uuid] = None
        try:
            yield ENV.process(self._prepare_image(instance_ref))
        except simpy.Interrupt:
            return

        instance_type = instance_ref['instance_type']
        instance = Instance(instance_uuid,
                            instance_type,
                            job_store,
                            self.resources)

        self.instance_uuids.append(instance_uuid)
        self.instances[instance_uuid] = instance

        # FIXME: this does not work. not safe
        MANAGER.change_status(instance_uuid, "ACTIVE", instance=instance)
        utils.write_start(instance_uuid)
        utils.print_("node", self.name, "spawns instance %s" % instance.name)

    def terminate_instance(self, instance_uuid):
        """Terminate the instance."""
        utils.print_("host", self.name, "terminates %s" % instance_uuid)
        # FIXME: This fails if we are downloading the image and the image
        # is not running yet
        try:
            instance = self.instances.pop(instance_uuid)
        except KeyError:
            # Instance not running, cancel the task
            if self.instance_tasks[instance_uuid]:
                self.instance_tasks[instance_uuid].interrupt()
        else:
            ENV.process(instance.shutdown())
            self.instance_uuids.remove(instance_uuid)

    def launch_instance(self, instance_uuid,
                        instance_ref, job_store):
        """Launch an instance if we can allocate it.

        We will raise a exception.NoValidHost if we cannot
        allocate resource for the image. FIXME(aloga): we should raise
        nova.exception.ComputeResourcesUnavailable ?
        """
        for i in ('vcpus', 'memory_mb', 'disk'):
            res = instance_ref['instance_type'][i]
            if res > self.resources[i].level:
                msg = ("cannot spawn instance ( %s > %s %s)" %
                       (res, self.resources[i].level, i))
                utils.print_("node", self.name, msg)
                raise exception.NoValidHost(reason=msg)

        try:
            ENV.process(self._create_instance(instance_uuid,
                                              instance_ref,
                                              job_store))
        except simpy.Interrupt:
            pass


def generate(reqs):
    """Generate the needed objects for the simulation.

    This function will generate the request objects from the traces and
    the hosts where the instances will be spawned
    """

    # FIME(aloga): really simplistic
    hosts = []
    for i in xrange(50):
        hosts.append(Host("node-%02d" % i,
                          2,
                          32 * 1024,
                          200 * 1024 * 1024))
    MANAGER.add_hosts(hosts)

    for req in reqs:
        if req["start"] != 0 and req["start"] < req["submit"]:
            print "discarding req %s" % req["id"]
            continue

        # FIXME(aloga). we should make this configurable. Or even adjust the
        # request to the available flavors.
        job_store = simpy.Store(ENV, capacity=1000)
        r = Request(req, job_store)
        ENV.process(r.do())
        yield ENV.timeout(0)


def start():
    """Start the simulation until max_time."""

    if not os.path.exists(CONF.simulator.output_dir):
        os.mkdir(CONF.simulator.output_dir)

    reqs = utils.load_requests(CONF.simulator.trace_file)
    if CONF.simulator.max_simulation_time is not None:
        max_time = CONF.simulator.max_simulation_time
    else:
        # FIXME(aloga): this does not work when downloading images...
        max_time = max([i["end"] for i in reqs]) * 30

    MANAGER.setUp()
    ENV.process(generate(reqs))
    # Start processes
    if max_time != 0:
        ENV.run(until=max_time)
    else:
        ENV.run()
