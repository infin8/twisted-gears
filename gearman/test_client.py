import struct
from collections import deque

from zope.interface import implements

from twisted.trial import unittest
from twisted.internet import interfaces, reactor, defer

import client, constants

class TestTransport(object):

    implements(interfaces.ITransport)

    disconnecting = False

    def __init__(self):
        self.received = []
        self.disconnected = 0

    def write(self, data):
        self.received.append(data)

    def writeSequence(self, data):
        self.received.extend(data)

    def loseConnection(self):
        self.disconnected += 1

    def getPeer(self):
        return None

    def getHost(self):
        return None

class ExpectedFailure(Exception):
    pass

class ProtocolTestCase(unittest.TestCase):

    def setUp(self):
        self.trans = TestTransport()
        self.gp = client.GearmanProtocol()
        self.gp.makeConnection(self.trans)

    def assertReceived(self, cmd, data):
        self.assertEquals(["\0REQ",
                           struct.pack(">II", cmd, len(data)),
                           data],
                          self.trans.received[:3])
        self.trans.received = self.trans.received[3:]

    def write_response(self, cmd, data):
        self.gp.dataReceived("\0RES")
        self.gp.dataReceived(struct.pack(">II", cmd, len(data)))
        self.gp.dataReceived(data)

class GearmanProtocolTest(ProtocolTestCase):

    def test_makeConnection(self):
        self.assertEquals(0, self.gp.receivingCommand)
        self.assertEquals([], list(self.gp.deferreds))
        self.assertEquals([], list(self.gp.unsolicited_handlers))

    def test_send_raw(self):
        self.gp.send_raw(11, "some data")
        self.assertReceived(11, "some data")
        self.assertEquals(0, len(self.gp.deferreds))

    def test_send(self):
        self.gp.send(11, "some data")
        self.assertReceived(11, "some data")
        self.assertEquals(1, len(self.gp.deferreds))

    def test_connectionLost(self):
        d = self.gp.send(11, "test")
        d.addCallback(lambda x: unittest.FailTest())
        d.addErrback(lambda x: x.trap(ExpectedFailure))
        self.gp.connectionLost(ExpectedFailure())
        return d

    def test_badResponse(self):
        self.assertEquals(0, self.trans.disconnected)
        self.trans.shouldLoseConnection = True
        self.gp.dataReceived("X" * constants.HEADER_LEN)
        reactor.callLater(0, self.assertEquals, 1, self.trans.disconnected)

    def test_pre_sleep(self):
        d = self.gp.pre_sleep()
        self.assertReceived(constants.PRE_SLEEP, "")

    def test_send_echo(self):
        d = self.gp.echo()
        self.assertReceived(constants.ECHO_REQ, "hello")

    def test_echoRt(self):
        """Test an echo round trip."""
        d = self.gp.echo()
        d.addCallback(lambda x:
                          self.assertEquals(x,
                                            (constants.ECHO_RES, "hello")))
        self.write_response(constants.ECHO_RES, "hello")
        return d

    def test_register_unsolicited(self):
        def cb(cmd, data):
            pass
        self.gp.register_unsolicited(cb)
        self.assertEquals(1, len(self.gp.unsolicited_handlers))
        self.gp.register_unsolicited(cb)
        self.assertEquals(1, len(self.gp.unsolicited_handlers))
        self.gp.register_unsolicited(lambda a,b: True)
        self.assertEquals(2, len(self.gp.unsolicited_handlers))

    def test_unregister_unsolicited(self):
        def cb(cmd, data):
            pass
        self.gp.register_unsolicited(cb)
        self.assertEquals(1, len(self.gp.unsolicited_handlers))
        self.gp.unregister_unsolicited(cb)
        self.assertEquals(0, len(self.gp.unsolicited_handlers))

    def test_unsolicitedCallbackHandling(self):
        d = defer.Deferred()
        self.gp.register_unsolicited(lambda cmd, data: d.callback(True))
        self.write_response(constants.WORK_COMPLETE, "test\0")
        return d

class GearmanJobTest(unittest.TestCase):

    def test_constructor(self):
        gj = client.GearmanJob("footdle\0dys\0some data")
        self.assertEquals("footdle", gj.handle)
        self.assertEquals("dys", gj.function)
        self.assertEquals("some data", gj.data)

        self.assertEquals("<GearmanJob footdle func=dys with 9 bytes of data>",
                          repr(gj))

class GearmanWorkerTest(ProtocolTestCase):

    def setUp(self):
        super(GearmanWorkerTest, self).setUp()
        self.gw = client.GearmanWorker(self.gp)

    def test_registerFunction(self):
        self.gw.registerFunction("awesomeness", lambda x: True)
        self.assertReceived(constants.CAN_DO, "awesomeness")

    def test_sendingJobResponse(self):
        job = client.GearmanJob("test\0blah\0junk")
        self.gw._send_job_res(constants.WORK_COMPLETE, job, "the value")
        self.assertReceived(constants.WORK_COMPLETE, "test\0the value")

    def test_sleep(self):
        a = []
        for i in range(5):
            a.append(self.gw._sleep())

        self.write_response(constants.NOOP, "")
        return defer.DeferredList(a)

    def test_getJob(self):
        d = self.gw.getJob()
        self.write_response(constants.JOB_ASSIGN,
                            "footdle\0funk\0args and stuff")
        def _handleJob(j):
            self.assertEquals("footdle", j.handle)
            self.assertEquals("funk", j.function)
            self.assertEquals("args and stuff", j.data)

        d.addCallback(_handleJob)
        return d

    def test_getJobWithWaiting(self):
        d = self.gw.getJob()
        self.write_response(constants.NO_JOB, "")
        self.write_response(constants.NOOP, "")
        self.write_response(constants.JOB_ASSIGN,
                            "footdle\0funk\0args and stuff")
        def _handleJob(j):
            self.assertEquals("footdle", j.handle)
            self.assertEquals("funk", j.function)
            self.assertEquals("args and stuff", j.data)

        d.addCallback(_handleJob)
        return d

    def test_getJobWhileAlreadyWaiting(self):
        sd = self.gw._sleep()
        d = self.gw.getJob()
        self.write_response(constants.NOOP, "")
        self.write_response(constants.JOB_ASSIGN,
                            "footdle\0funk\0args and stuff")
        def _handleJob(j):
            self.assertEquals("footdle", j.handle)
            self.assertEquals("funk", j.function)
            self.assertEquals("args and stuff", j.data)

        d.addCallback(_handleJob)
        return defer.DeferredList([sd, d])

    def test_finishJob(self):
        self.gw.functions['blah'] = lambda x: x.upper()
        job = client.GearmanJob("test\0blah\0junk")
        d = self.gw._finishJob(job)

        d.addCallback(lambda x:
                          self.assertReceived(constants.WORK_COMPLETE,
                                              "test\0JUNK"))

    def test_finishJobNull(self):
        self.gw.functions['blah'] = lambda x: None
        job = client.GearmanJob("test\0blah\0junk")
        d = self.gw._finishJob(job)

        d.addCallback(lambda x:
                          self.assertReceived(constants.WORK_COMPLETE,
                                              "test\0"))

    def test_finishJobException(self):
        def _failing(x):
            raise Exception("failed")
        self.gw.functions['blah'] = _failing
        job = client.GearmanJob("test\0blah\0junk")
        d = self.gw._finishJob(job)

        def _checkReceived(x):
            self.assertReceived(constants.WORK_EXCEPTION,
                                "test\0" + 'Exception(failed)')
            self.assertReceived(constants.WORK_FAIL, "test\0")

        d.addCallback(_checkReceived)

    def test_doJob(self):
        self.gw.functions['blah'] = lambda x: x.upper()
        d = self.gw.doJob()
        self.write_response(constants.JOB_ASSIGN,
                            "footdle\0blah\0args and stuff")

        def _verify(x):
            self.assertReceived(constants.GRAB_JOB, "")
            self.assertReceived(constants.WORK_COMPLETE,
                                "footdle\0ARGS AND STUFF")

        d.addCallback(_verify)
        return d

class GearmanJobHandleTest(unittest.TestCase):

    def test_workData(self):
        gjh = client.GearmanJobHandle(None)
        gjh._work_data.extend(['test', 'ing'])
        self.assertEquals('testing', gjh.work_data)

    def test_workWarning(self):
        gjh = client.GearmanJobHandle(None)
        gjh._work_warning.extend(['test', 'ing'])
        self.assertEquals('testing', gjh.work_warning)
