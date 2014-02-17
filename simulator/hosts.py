import datetime
import random

import nova.exception
from oslo.config import cfg
import simpy

import simulator
import simulator.catalog
import simulator.instances
from simulator import log as logging
import simulator.scheduler
import simulator.utils

CONF = cfg.CONF

ENV = simulator.ENV

MANAGER = simulator.scheduler.manager
CATALOG = simulator.catalog.CATALOG


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

        self.LOG = logging.getLogger(__name__, {"id": self.name})

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

        self.LOG.debug("download of %s starts" % image_uuid)
        # Download the image from the catalog
        yield ENV.process(CATALOG.download(image))
        self.LOG.debug("download of %s ends" % image_uuid)
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
        instance = simulator.instances.Instance(instance_uuid,
                                                instance_type,
                                                job_store,
                                                self.resources)

        self.instance_uuids.append(instance_uuid)
        self.instances[instance_uuid] = instance

        # FIXME: this does not work. not safe
        MANAGER.change_status(instance_uuid, "ACTIVE", instance=instance)
        simulator.utils.write_start(instance_uuid)
        self.LOG.info("spawns instance %s" % instance.name)

    def terminate_instance(self, instance_uuid):
        """Terminate the instance."""
        self.LOG.info("terminates %s" % instance_uuid)
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
                self.LOG.error(msg)
                raise nova.exception.NoValidHost(reason=msg)

        try:
            ENV.process(self._create_instance(instance_uuid,
                                              instance_ref,
                                              job_store))
        except simpy.Interrupt:
            pass

