import random

import simpy

import simulator
from simulator import log as logging

ENV = simulator.ENV

LOG = logging.getLogger(__name__)


class Catalog(object):
    """The catalog serves images to hosts."""
    def __init__(self, name):
        self.name = name

        self.bw = 1.0  # Gbit
        self.chunk_size = 256.0
        self.downloads = simpy.Container(ENV)

    def download(self, image):
        yield self.downloads.put(1)

        LOG.info("serving %(uuid)s, %(size)fG" % image)

        size = image["size"] * 8 * 1024
        served = 0
        while served < size:
            download_nr = self.downloads.level
            variation = random.uniform(0.8, 0.9)
            bw = variation * self.bw * 1024
            bw = bw / download_nr
            download_time = self.chunk_size / bw
            yield ENV.timeout(download_time)
            served += self.chunk_size

        yield self.downloads.get(1)


CATALOG = Catalog("catalog")
