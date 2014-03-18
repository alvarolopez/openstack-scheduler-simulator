import uuid

import simulator
from simulator import catalog
from simulator import test

ENV = simulator.ENV


class CatalogTest(test.TestCase):
    def setUp(self):
        super(CatalogTest, self).setUp()
        self.catalog = catalog.Catalog("catalog")

    def test_defaults(self):
        self.assertIsNotNone(self.catalog.name)
        self.assertEqual(1.0, self.catalog.bw)
        self.assertEqual(256.0, self.catalog.chunk_size)

    def test_download(self):
        image = {"size": 10.0,
                 "uuid": uuid.uuid4().hex}
        ENV.process(self.catalog.download(image))
        ENV.run()
        now = ENV.now
        self.assertTrue(90 < now < 100)
