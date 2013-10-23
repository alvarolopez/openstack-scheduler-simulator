# vim: tabstop=4 shiftwidth=4 softtabstop=4

import uuid

from nova import context as nova_context
from nova import exception
from nova.scheduler import manager
from nova.scheduler import utils

import simulator.cloud
from simulator import fakes

exception

def create_request_spec(flavor, image, instance_nr):
    """Create a request spec according"""
    instances = []
    for i in xrange(instance_nr):
        instance = {"uuid": uuid.uuid4().hex}
        instances.append(instance)

    instance_type = fakes.flavors.get_flavor_by_name(flavor)

    request_spec = utils.build_request_spec(None, # Context
                                            image,
                                            instances,
                                            instance_type)
    return request_spec


class SchedulerManager(object):
    def __init__(self, env):
        """Wrap around the nova scheduler."""
        self.env = env
        self.manager = manager.SchedulerManager(scheduler_driver="nova.scheduler.chance.ChanceScheduler")

    def run_instance(self, request_spec):
        """Run an instance on a node"""
        simulator.cloud.print_("scheduler", "", "got request %s" % request_spec)

        context = nova_context.RequestContext(user_id=None,
                                              project_id=None,
                                              is_admin=False,
                                              read_deleted='no',
                                              overwrite=False)

        self.manager.run_instance(context=context,
                                  request_spec=request_spec,
                                  admin_password=None,
                                  injected_files=None,
                                  requested_networks=None,
                                  is_first_time=False,
                                  filter_properties={},
                                  legacy_bdm_in_spec=None)

    def terminate_instance(self, uuid):
        pass
