import cStringIO

import mock

from simulator import test
from simulator import utils


## FIXME(aloga): this is just a skel
#class UtilsTest(test.TestCase):
#    GOOD_REQS = (
#        ('#1', 'foo', '1', '2', '3', '4', 'fooimage', '1', 'm1.tiny'),
#        ('1', 'user', '1', '2', '3', '4', 'fooimage', '1', 'm1.tiny'),
#        ('2', 'user', '1', '2', '3', '4', 'fooimage', '', ''),
#    )
##    def fake_open(self, req):
##        return cStringIO.StringIO("\n".join([','.join(list(i)) for i in req])
#    def test_load_requests(self):
#        my_mock = mock.MagicMock()
#
#        with mock.patch('__builtin__.open') as  my_mock:
#            my_mock.return_value.__enter__ = lambda x: cStringIO.StringIO(
#                "\n".join([','.join(list(i)) for i in self.GOOD_REQS]))
#            my_mock.return_value.__exit__ = mock.Mock()
##            my_mock.return_value.read.return_value = cStringIO.StringIO("KK")
#
#            reqs = utils.load_requests("foobar")
#        for req in reqs:
#            print req
#            self.assertEqual('owner', req['owner'])
