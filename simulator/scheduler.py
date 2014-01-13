# vim: tabstop=4 shiftwidth=4 softtabstop=4

import uuid

import fixtures
from oslo.config import cfg

from nova import context as nova_context
from nova import exception
import nova.scheduler.driver
import nova.scheduler.manager
import nova.scheduler.utils

from simulator import utils

CONF = cfg.CONF


def create_request_spec(flavor, image, instance_nr):
    """Create a request spec according"""
    instances = []
    for i in xrange(instance_nr):
        instance = {"uuid": uuid.uuid4().hex}
        instances.append(instance)

    instance_type = flavors.get_flavor_by_name(flavor)

    request_spec = nova.scheduler.utils.build_request_spec(None,  # Context
                                                           image,
                                                           instances,
                                                           instance_type)
    return request_spec


class Flavors(object):
    INSTANCE_TYPES_MAP = {
        1: "m1.small",
        "m1.small": 1
    }

    INSTANCE_TYPES = {
        "m1.small": {"cpus": 4,
                     "mem": 4 * 1024,
                     "disk": 20 * 1024 * 1024,
                     "flavorid": 1,
                     },
    }

    def extract_flavor(self):
        return

    def get_default_flavor(self):
        return self.INSTANCE_TYPES.get("m1.small")

    def get_flavor_by_name(self, name, ctxt=None):
        """Retrieves single flavor by name."""
        if name is None:
            return self.get_default_flavor()

        return self.INSTANCE_TYPES.get(name)

    def get_flavor(self, instance_type_id, ctxt=None, inactive=False):
        """Retrieves single flavor by id."""
        if instance_type_id is None:
            return self.get_default_flavor()

        name = self.INSANTE_TYPES_MAP.get(instance_type_id)
        return self.INSTANCE_TYPES.get(name)


flavors = Flavors()


class FakeEventReporter(object):
    """Fake Context manager."""

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class FakeSchedulerBaseClass(nova.scheduler.driver.Scheduler):
    def hosts_up(self, context, topic):
        return manager.get_hosts()


def blank_fn(name):
    def fn(*args, **kwargs):
        pass
#        print("called %s" % name)
    return fn


class SchedulerManager(fixtures.Fixture):
    def __init__(self):
        """Wrap around the nova scheduler."""
        super(SchedulerManager, self).__init__()

        self.manager = None
        self.hosts = {}
        self.instances = {}

    def _monkey_patch(self, monkey, patch):
        self.useFixture(fixtures.MonkeyPatch(monkey, patch))

    def setUp(self):
        super(SchedulerManager, self).setUp()
        # utils.build_request_spec
        self._monkey_patch('nova.compute.flavors',
                           flavors)

        self._monkey_patch('nova.db.flavor_extra_specs_get',
                           lambda *args: [])

        # SchedulerManager
        self._monkey_patch('nova.compute.utils.EventReporter',
                           FakeEventReporter)

        # Fixture the base class instead
#        self._monkey_patch('nova.scheduler.driver.Scheduler.get_hosts',
#                           self.get_hosts)

        self._monkey_patch('nova.scheduler.driver.Scheduler',
                           FakeSchedulerBaseClass)

        self._monkey_patch('nova.scheduler.driver.handle_schedule_error',
                           self._fake_handle_schedule_error)

        self._monkey_patch('nova.scheduler.driver.instance_update_db',
                           self._fake_db_instance_update)

        self._monkey_patch('nova.compute.rpcapi.ComputeAPI.run_instance',
                           self._fake_run_instance)

        self.manager = nova.scheduler.manager.SchedulerManager(
            scheduler_driver=CONF.scheduler_driver)

    def _fake_handle_schedule_error(self, context, ex,
                                    instance_uuid, request_spec):
        if not isinstance(ex, exception.NoValidHost):
            msg = "Exception during schduler run: %s" % ex.message
        else:
            self.instances[instance_uuid]["status"] = "ERROR"
            msg = ("setting instance %s to error (%s)" %
                   (instance_uuid, ex.message))
        utils.print_("scheduler", "", msg)

    def _fake_db_instance_update(self, context,
                                 instance_uuid, extra_values=None):
        return instance_uuid

    def _fake_run_instance(self, *args, **kwargs):
        instance_ref = kwargs["request_spec"]
        instance_uuid = kwargs["instance"]
        host = kwargs["host"]
        job_store = self.instances[instance_uuid]["job_store"]
        self.instances[instance_uuid]["host"] = host.name
        self.change_status(instance_uuid, "BUILD")
        host.launch_instance(instance_uuid, instance_ref, job_store)

    def change_status(self, instance_uuid, status):
        self.instances[instance_uuid]["status"] = status

    def add_hosts(self, hosts):
        for i in hosts:
            self.hosts[i.name] = i

    def get_hosts(self):
        return self.hosts.values()

    def run_instance(self, request_spec, job_store):
        """Run an instance on a node"""
        utils.print_("scheduler", "", "got req %s" % request_spec)

        for instance_uuid in request_spec["instance_uuids"]:
            self.instances[instance_uuid] = {"host": None,
                                             "status": None,
                                             "job_store": job_store}
            self.change_status(instance_uuid, "SCHEDULE")

        context = nova_context.RequestContext(user_id=None,
                                              project_id=None,
                                              is_admin=False,
                                              read_deleted='no',
                                              overwrite=False)

        return self.manager.run_instance(context=context,
                                         request_spec=request_spec,
                                         admin_password=None,
                                         injected_files=None,
                                         requested_networks=None,
                                         is_first_time=False,
                                         filter_properties={},
                                         legacy_bdm_in_spec=None)

    def terminate_instance(self, instance_uuid):
        instance_state = self.instances.get(instance_uuid, None)
        if (instance_state and instance_state["status"] in ("ACTIVE",
                                                            "ERROR",
                                                            "BUILD")):
            host = instance_state["host"]
            if instance_state["status"] in ("ACTIVE", "BUILD"):
                self.hosts[host].terminate_instance(instance_uuid)
            self.change_status(instance_uuid, "DELETED")
        else:
            utils.print_("scheduler",
                         "",
                         "unknown instance %s" % instance_uuid)


manager = SchedulerManager()