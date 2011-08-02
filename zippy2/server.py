#!/usr/bin/env python
from twisted.internet import reactor
from twisted.web.server import Site

from zippy2.api import ApiRoot


def main():
    factory = Site(ApiRoot)
    reactor.listenTCP(9999, factory)
    reactor.run()


if __name__ == '__main__':
    main()
