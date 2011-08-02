from twisted.internet.defer import inlineCallbacks
from twisted.trial import unittest

from zippy2.consumers import Crc32Consumer
from zippy2.producers import FileProducer
from zippy2.tests.test_producers import ProducerMixin
from zippy2.tests.test_producers import make_random_temp_file



class test_CRC32Consumer(ProducerMixin, unittest.TestCase):

    @inlineCallbacks
    def test_crc32(self):
        filename = make_random_temp_file()
        self.tempfiles.append(filename)

        consumer = Crc32Consumer()
        producer = FileProducer(filename)
        
        yield producer.beginProducing(consumer)
        
        self.assertEquals(consumer.crc32(), producer.crc32())


