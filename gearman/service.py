from twisted.application import internet, service
from twisted.internet import reactor, protocol, task

import client, cjson, traceback

class GearmanWorkerFactory(protocol.ClientFactory):
    protocol = client.GearmanProtocol

    def __init__(self, concurrency=5, id=None):
        self.concurrency = concurrency
        self.id = id
        self.functions = []

    def clientConnectionLost(self, connector, reason):
        """If we get disconnected, reconnect to server."""
        connector.connect()

    def clientConnectionFailed(self, connector, reason):
        print "connection failed:", reason
        reactor.stop()
    
    def addFunction(self, func, name=None):
        name = name or func.__name__
        
        self.functions.append((name, expose(func)))
    
    def startWork(self, gm):
        w = client.GearmanWorker(gm)
        
        if self.id: w.setId(self.id)
        
        for name, func in self.functions:
            w.registerFunction(name, func)
        
        coop = task.Cooperator()
        
        for i in range(self.concurrency):
            reactor.callLater(0.1 * i, lambda: coop.coiterate(w.doJobs()))

class GearmanWorkerService(service.Service):
    def __init__(self, host, port, factory):
        self.host = host
        self.port = int(port)
        self.factory = factory
    
    def startService(self):
        reactor.callWhenRunning(self.startListening)
    
    def startListening(self):
        c = protocol.ClientCreator(reactor, self.factory.protocol)
        d = c.connectTCP(self.host, self.port)
        d.addCallback(self.factory.startWork)

def expose(f):
    def wrapped(job):
        args = cjson.decode(job)
        
        try:
            d = f(*args)
            d.addCallback(cjson.encode)
            return d
        except Exception, e:
            return cjson.encode({'error': traceback.print_tb(e) + str(e)})
    
    return wrapped