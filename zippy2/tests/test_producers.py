from twisted.trial import unittest
from zope.interface import implements
from twisted.internet.interfaces import IConsumer
from twisted.internet.defer import inlineCallbacks

from zippy2.producers import FileProducer
from zippy2.producers import AwsProducer

from StringIO import StringIO

import tempfile
import random
import binascii
import os

class TestConsumer(StringIO):
    implements(IConsumer)

    def registerProducer(self, producer, streaming):
        pass

    def unregisterProducer(self):
        pass

    def finish(self):
        pass


class test_FileProducer(unittest.TestCase):

    @inlineCallbacks
    def test_fields(self):
        fd, filename = tempfile.mkstemp(prefix='z2')
        fh = os.fdopen(fd, 'w')
        with open('/dev/urandom', 'r') as r:
            data = r.read(5*(1024**2))  # 5MB of random data
            fh.write(data)
        fh.flush()
        fh.close()

        producer = FileProducer(filename)
        size = yield producer.size()
        crc32 = yield producer.crc32()

        self.assertEquals(os.path.getsize(filename), size)
        self.assertEquals(binascii.crc32(data) & 0xffffffff, crc32)

        consumer = TestConsumer()
        yield producer.beginProducing(consumer)

        consumer.seek(0)
        self.assertEquals(data, consumer.read())


