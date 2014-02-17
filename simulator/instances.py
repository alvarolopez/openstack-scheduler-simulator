import random

import nova.exception
import simpy

import simulator
from simulator import log as logging

ENV = simulator.ENV


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
        self.LOG = logging.getLogger(__name__, {"id": self.name})

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
                    raise nova.exception.ComputeResourcesUnavailable()
            self.LOG.debug("consumes %s (%s left) %s" %
                                (amount,
                                 self.node_resources[resource].level,
                                 resource))

        # Spawn
        self.LOG.warning("starts w/ %s vcpus" % self.vcpus)
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
        self.LOG.info("finishes")

        # Free the node_resources
        # FIXME(DRY)
        for resource in ("vcpus", "memory_mb", "root_gb", "ephemeral_gb"):
            amount = getattr(self, resource, 0)
            if amount == 0:
                continue

            if resource in ("root_gb", "ephemeral_gb"):
                resource = "disk"

            self.node_resources[resource].put(amount)
            self.LOG.debug("frees %s %s" % (amount, resource))

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

