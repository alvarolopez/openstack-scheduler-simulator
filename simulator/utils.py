import simulator

ENV = simulator.ENV


def print_(who, id, what):
    time = ENV.now
    id = id[:15]
    print("%(time)2.1f %(who)10s %(id)15s %(what)s" %
          {"time": time, "who": who, "id": id, "what": what})


def load_requests(file):
    """Load requests from file."""
    reqs = []
    with open(file) as f:
        for req in f.readlines():
            req = req.strip()
            if req.startswith("#"):
                continue
            req = req.split()
            reqs.append({"id": req[0],
                         "owner": req[1],
                         "submit": float(req[2]),
                         "start": float(req[3]),
                         "end": float(req[4]),
                         "tasks": int(req[5]), })
    return reqs
