#!/usr/bin/env python
from twisted.internet import reactor
from twisted.web.server import Site
from twisted.application.service import Service

from zippy2.api import ApiRoot

class StreamingServiceApi(Service):

    def startService(self, port=11000):
        factory = Site(ApiRoot)
        reactor.listenTCP(port, factory)

    def stopService(self):
        self.uploadWatcher.destroy_watcher()


