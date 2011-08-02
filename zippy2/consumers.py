from twisted.internet.interfaces import IConsumer

from zope.interface import implements

import binascii


class Crc32Consumer(object):
    implements(IConsumer)

    def __init__(self):
        self._crc32 = 0

    def crc32(self):
        return self._crc32

    def write(self, data):
        self._crc32 = binascii.crc32(data, self._crc32) & 0xffffffff

    def registerProducer(self, producer, streaming):
        pass

    def unregisterProducer(self):
        pass



