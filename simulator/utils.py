import simulator

ENV = simulator.ENV


def print_(who, id, what):
    time = ENV.now
    id = id[:15]
    print("%(time)2.1f %(who)10s %(id)15s %(what)s" %
          {"time": time, "who": who, "id": id, "what": what})
