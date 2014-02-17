import simulator
from simulator import log as logging

ENV = simulator.ENV


class SimpleJob(object):
    """One Job executed inside an instance."""
    def __init__(self, jid, wall):
        self.name = "j-%s" % jid

        self.wall = wall
        self.finished = ENV.event()

        self.LOG = logging.getLogger(__name__, {"id": self.name})

    def start(self):
        if self.wall != 0:
            ENV.process(self.do())

    def do(self):
        """Do the job."""
        self.LOG.info("starts (wall %s)" % self.wall)
        # Now consume the walltime
        yield ENV.timeout(self.wall)
        self.LOG.info("ends (wall %s)" % self.wall)
        self.finished.succeed()
