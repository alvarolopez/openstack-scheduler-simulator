import random

import simpy

import simulator
from simulator import log as logging

ENV = simulator.ENV


class Instance(object):
    """An instance.

    It can run several jobs ( #jobs <= #vcpus )
    """

    def __init__(self, name, instance_type, job_store):
        self.name = name

        self.job_store = job_store
        self.vcpus = instance_type["vcpus"]
        self.memory_mb = instance_type["memory_mb"]
        self.root_gb = instance_type["root_gb"]
        self.ephemeral_gb = instance_type["ephemeral_gb"]

        self.boot_time = round(random.uniform(10, 20))

        self.finished = ENV.event()

        # Boot the machine
        self.process = ENV.process(self.boot())

        self.LOG = logging.getLogger(__name__, {"id": self.name})

    def boot(self):
        """Simulate the boot process.

        Before actually booting the image, we will consume resources from
        the host.
        """
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
