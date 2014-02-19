from oslo.config import cfg

import simulator
import simulator.scheduler
from simulator import log as logging

CONF = cfg.CONF

ENV = simulator.ENV
MANAGER = simulator.scheduler.manager


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

        self.LOG = logging.getLogger(__name__, {"id": self.name})

    def do(self):
        # We wait until the request is made by the user. In SimPy2 it was done
        # by activating the process at a given time using the kwarg "at=", no
        # longer present
        yield ENV.timeout(self.req["submit"] - ENV.now)
        start = ENV.now
        self.LOG.info("start w/ %s cores", self.req["cores"])
        yield ENV.timeout(1)

        # Prepare the jobs
        jid = self.req["id"]
        wall = self.req["end"] - self.req["start"]
        expected_elapsed = self.req["end"] - self.req["submit"]
        if wall > 0:
            for i in xrange(self.req["cores"]):
                job = simulator.jobs.SimpleJob("%s-%s" % (jid, i), wall)
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

        self.LOG.debug("%s got instances: %s" % (self.name, instance_uuids))

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
            what = yield ENV.all_of([i["instance"].finished
                                     for i in instances])

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
        self.LOG.info(msg)
