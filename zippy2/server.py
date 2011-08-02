#!/usr/bin/env python
from twisted.internet import reactor
from twisted.internet.interfaces import IPushProducer
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import inlineCallbacks
from twisted.internet.defer import returnValue

from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.resource import Resource

from zippy2.api import ApiRoot


def main():
    factory = Site(ApiRoot)
    reactor.listenTCP(9999, factory)
    reactor.run()


if __name__ == '__main__':
    main()
