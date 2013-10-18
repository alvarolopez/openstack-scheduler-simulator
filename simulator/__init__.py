# vim: tabstop=4 shiftwidth=4 softtabstop=4

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
                         "tasks": int(req[5]),
            })
    return reqs
