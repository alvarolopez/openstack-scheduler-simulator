import sys

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
        "id": (0, int, True),
        "ownwer": (1, str, True),
        "submit": (2, int, True),
        "terminate": (3, int, False),
        "start": (4, int, False),
        "end": (5, int, False),
        "cores": (6, int, True),
        "image": (7, str, False),
        "size": (8, float, False),
        "flavor": (9, str, False),

    }

    reqs = []
    with open(file) as f:
        for line in f.readlines():
            line = line.strip()
            if line.startswith("#"):
                continue

            line = [i.strip() for i in line.split(",")]

            if len(line) != len(fields):
                print >> sys.stderr, "discarding request %s" % ",".join(line)
                print >> sys.stderr, "ERROR: incorrect number of fields"
                sys.exit(1)

            req = {}

            for field, (position, trans, required) in fields.iteritems():
                if line[position] == "":
                    if not required:
                        if trans in (int, float):
                            req[field] = 0
                        else:
                            req[field] = ""
                    else:
                        print >> sys.stderr, ("discarding request %s" %
                            ",".join(line))
                        print >> sys.stderr, ("Bad trace file, missing "
                                              "required field %s" %
                                              field)
                        sys.exit(1)
                else:
                    req[field] = trans(line[position])
            reqs.append(req)

    return reqs
