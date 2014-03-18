import fixtures
import testtools


class TestCase(testtools.TestCase, fixtures.TestWithFixtures):
    def setUp(self):
        """Run before each test method to initialize test environment."""
        super(TestCase, self).setUp()
