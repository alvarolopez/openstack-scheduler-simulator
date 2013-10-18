# vim: tabstop=4 shiftwidth=4 softtabstop=4

import fixtures

import nova.scheduler.driver


class Flavors(object):
    INSTANCE_TYPES_MAP = {
        1: "m1.small",
        "m1.small": 1
    }

    INSTANCE_TYPES = {
        "m1.small": {"cpus": 4, "flavorid": 1},
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
        return fixture.get_hosts()


def blank_fn(name):
    def fn(*args, **kwargs):
        pass
#        print("called %s" % name)
    return fn

class Fixture(fixtures.Fixture):
    def __init__(self):
        super(Fixture, self).__init__()
        self.hosts = []

    def _monkey_patch(self, monkey, patch):
        self.useFixture(fixtures.MonkeyPatch(monkey, patch))

    def setUp(self):
        super(Fixture, self).setUp()
        # utils.build_request_spec
        self._monkey_patch('nova.compute.flavors',
                           flavors)
        self._monkey_patch('nova.db.flavor_extra_specs_get',
                           lambda *args: [])

        # SchedulerManager
        self._monkey_patch('nova.compute.utils.EventReporter',
                           FakeEventReporter)

#        self.useFixture(fixtures.MonkeyPatch(',#
        self._monkey_patch('nova.scheduler.driver.Scheduler',
                           FakeSchedulerBaseClass)

        self._monkey_patch('nova.scheduler.driver.instance_update_db',
                           blank_fn('nova.scheduler.driver.instance_update_db'))
        # FIXME(aloga): this does not work
        self._monkey_patch('nova.compute.rpcapi.ComputeAPI.run_instance',
                           self.fake_run_instance)

    def fake_run_instance(*args, **kwargs):
        host = kwargs["host"]
        instance_ref = kwargs["request_spec"]
        host.launch_instance(instance_ref)

    def add_hosts(self, hosts):
        self.hosts = hosts

    def get_hosts(self):
        return self.hosts

fixture = Fixture()
fixture.setUp()

def add_hosts(hosts):
    """Add hosts to the fixture."""
    fixture.add_hosts(hosts)
