import simulator

ENV = simulator.ENV


def print_(who, id, what):
    time = ENV.now
    id = id[:15]
    print("%(time)2.1f %(who)10s %(id)15s %(what)s" %
          {"time": time, "who": who, "id": id, "what": what})


def load_requests(file):
    """Load requests from file."""

    fields = {
        "id": (0, int),
        "ownwer": (1, str),
        "submit": (2, int),
        "start": (3, int),
        "end": (4, int),
        "terminate": (5, int),
        "cores": (6, int),
        "image": (7, str),
        "size": (8, float),
        "flavor": (9, str),

    }

    reqs = []
    with open(file) as f:
        for line in f.readlines():
            line = line.strip()
            if line.startswith("#"):
                continue

            line = [i.strip() for i in line.split(",")]

            if len(line) != len(fields):
                print "discarding request %s" % ",".join(line)
                continue

            req = {}

            for field, (position, trans) in fields.iteritems():
                req[field] = trans(line[position])
            reqs.append(req)

    return reqs
