import datetime
import os.path
import sys

from oslo.config import cfg

import simulator

CONF = cfg.CONF

ENV = simulator.ENV

OUTPUT_PREFIX = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
OUTPUT_PREFIX += "_"


def write_to_file(outfile, output):
    """Write to an output file."""
    outfile = os.path.join(CONF.simulator.output_dir,
                           OUTPUT_PREFIX + outfile)

    with open(outfile, "a") as f:
        f.write(output)


def write_start(uuid):
    now = ENV.now
    output = "%(req_id)s\t%(start)s\n" % {"req_id": uuid,
                                          "start": now}
    write_to_file("start", output)


def load_requests(filename):
    """Load requests from file."""

    # I could use CVS here, but I prefer this methid
    fields = {
        "id": (0, int, True),
        "ownwer": (1, str, True),
        "submit": (2, int, True),
        "timeout": (3, int, False),
        "terminate": (4, int, False),
        "cores": (5, int, True),
        "image": (6, str, False),
        "size": (7, float, False),
        "flavor": (8, str, False),

    }

    reqs = []
    with open(filename) as f:
        for line in f.readlines():
            line = line.strip()

            if line.startswith("#"):
                continue

            line = [i.strip() for i in line.split(",")]

            if len(line) != len(fields):
                print >> sys.stderr, ("ERROR: incorrect number of fields on "
                                      "request: \n\t%s" % ",".join(line))
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
                                              "required field %s" % field)
                        sys.exit(1)
                else:
                    req[field] = trans(line[position])
            reqs.append(req)

    return reqs
