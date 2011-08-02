from twisted.trial import unittest
from zope.interface import implements
from twisted.internet.interfaces import IConsumer
from twisted.internet.defer import inlineCallbacks
from twisted.internet.defer import DeferredList
from txaws.s3.client import S3Client

from zippy2.producers import FileProducer
from zippy2.producers import AwsProducer

from StringIO import StringIO

import tempfile
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


def make_random_temp_file(size=5*(1024**2)):
    fd, filename = tempfile.mkstemp(prefix='z2')
    fh = os.fdopen(fd, 'w')
    with open('/dev/urandom', 'r') as r:
        data = r.read(5*(1024**2))  # 5MB of random data
        fh.write(data)
    fh.flush()
    fh.close()
    return filename


class ProducerMixin(object):
    def setUp(self):
        super(ProducerMixin, self).setUp()
        self.tempfiles = []
        self.s3keys = []

    def tearDown(self):
        super(ProducerMixin, self).tearDown()
        for temp in self.tempfiles:
            os.unlink(temp)

        s3client = S3Client()
        deletelist = []
        for key in self.s3keys:
            d = s3client.delete_object('zippy2-dev', key)
            deletelist.append(d)

        return DeferredList(deletelist)



class test_FileProducer(ProducerMixin, unittest.TestCase):

    @inlineCallbacks
    def test_fields(self):
        filename = make_random_temp_file()
        self.tempfiles.append(filename)
        producer = FileProducer(filename)
        size = yield producer.size()
        crc32 = yield producer.crc32()

        with open(filename) as f:
            data = f.read()

        self.assertEquals(os.path.getsize(filename), size)
        self.assertEquals(binascii.crc32(data) & 0xffffffff, crc32)

        consumer = TestConsumer()
        yield producer.beginProducing(consumer)

        consumer.seek(0)
        self.assertEquals(data, consumer.read())


class test_AwsProducer(ProducerMixin, unittest.TestCase):

    @inlineCallbacks
    def test_fields(self):
        filename = make_random_temp_file()
        key = os.path.basename(filename)

        self.tempfiles.append(filename)
        self.s3keys.append(key)

        with open(filename) as f:
            data = f.read()
            crc32 = binascii.crc32(data) & 0xffffffff

        s3client = S3Client()
        yield s3client.put_object('zippy2-dev', key, data, metadata={'crc32': '%x' % crc32})

        consumer = TestConsumer()
        producer = AwsProducer('zippy2-dev', key)
        yield producer.beginProducing(consumer)

        consumer.seek(0)
        self.assertEquals(data, consumer.read())
        producer_crc32 = yield producer.crc32()
        producer_size = yield producer.size()
        self.assertEquals(crc32, producer_crc32)
        self.assertEquals(len(data), producer_size)


