import SimPy.Simulation as sim

## Model components -----------------------------


class Source(sim.Process):
    """Create requests from the traces"""
    def generate(self, jobs):
        for job_info in jobs:
            if job_info["start"] < job_info["submit"]:
                print "discarding job %s" % job_info["id"]
                continue
            job = Job(job_info)
            sim.activate(job, job.do(), at=job_info["start"])
            r = Request(name=job_info["id"], job=job)
            sim.activate(r, r.request(), at=job_info["submit"])
            yield sim.hold, self, 0


class Request(sim.Process):
    """A request"""
    def __init__(self, name, job):
        super(Request, self).__init__(name)
        self.job = job

    def request(self):
        print("%2.1f %s > request starts %s nodes" % (sim.now(), self.name, self.job.nodes))
        yield sim.waitevent, self, self.job.event
        print("%2.1f %s < request end" % (sim.now(), self.name))


class Job(sim.Process):
    """One Job in the batch system"""
    def __init__(self, job_info):
        name = job_info["id"]
        super(Job, self).__init__(name)
        self.start = job_info["start"]
        self.end = job_info["end"]
        self.nodes = job_info["nodes"]
        self.wall = self.end - self.start
        self.event = sim.SimEvent("finish job %s" % self.name)

    def do(self):
        # Start when we're supposed to start
        print("%2.1f %s  > job starts (wall %s)" % (sim.now(), self.name, self.wall))
        yield sim.hold, self, self.start - sim.now()
        # Now consume the walltime
        yield sim.hold, self, self.wall
        self.event.signal(self.name)
        print("%2.1f %s  < job ends (wall %s)" % (sim.now(), self.name, self.wall))


## Experiment data ------------------------------

jobs = []
with open("data/trace.dat") as f:
    for job in f.readlines():
        job = job.split()
        jobs.append({"id": job[0],
                     "owner": job[1],
                     "submit": float(job[2]),
                     "start": float(job[3]),
                     "end": float(job[4]),
                     "nodes": int(job[5]),
        })

maxTime = jobs[-1]["end"]

## Model/Experiment ------------------------------

sim.initialize()
s = Source()
sim.activate(s, s.generate(jobs), at=0.0)
sim.simulate(until=maxTime)
