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
            req = [i.strip() for i in req.split(",")]
            try:
                reqs.append({
                    "id": req[0],
                    "owner": req[1],
                    "submit": int(req[2]),
                    "start": int(req[3]) if req[3] else 0,
                    "end": int(req[4]) if req[4] else 0,
                    "tasks": int(req[5]),
                    "image": req[6],
                    "size": float(req[7]) if req[7] else 0.0,
                    "flavor": req[8],
                })
            except IndexError:
                print "discarding request %s" % req.join(",")

    return reqs
