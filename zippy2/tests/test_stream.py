from twisted.trial import unittest
from twisted.internet.defer import DeferredList
from twisted.internet.defer import returnValue
from twisted.internet.interfaces import IConsumer
from twisted.internet.defer import inlineCallbacks

from zope.interface import implements

from StringIO import StringIO
from zipfile import ZipInfo
from zipfile import ZipFile
import datetime
import random
import binascii
import tempfile
import os

from zippy2.producers import FileProducer
from zippy2.stream import ZipStream
from zippy2.stream import dos_timestamp
from zippy2.tests.test_producers import TestConsumer
from zippy2.tests.test_producers import make_random_temp_file

def MB(i):
    return int(i*(1024**2))

class ZipStreamMixin(object):

    def setUp(self):
        super(ZipStreamMixin, self).setUp()
        self.tempfiles = []

    def tearDown(self):
        super(ZipStreamMixin, self).tearDown()
        for temp in self.tempfiles:
            os.unlink(temp)


    @inlineCallbacks
    def create_zipstream_from_tempfiles(self, num_files, min_size=0, max_size=10*(1024**2)):
        consumer = TestConsumer()
        zipstream = ZipStream(consumer)
        deferreds = []
        producers = {}

        for i in range(num_files):
            producersize = random.randint(min_size, max_size)
            filename = make_random_temp_file(producersize)

            producer = FileProducer(filename)
            deferreds.append(zipstream.addProducer(producer))
            producers.update({ filename: producer })
            self.tempfiles.append(filename)

        deferredlist = DeferredList(deferreds)
        deferredlist.addCallback(lambda _: zipstream.centralDirectory())
        deferredlist.addCallback(lambda _: consumer.seek(0))
        yield deferredlist

        returnValue((zipstream, consumer, producers))
            

    @inlineCallbacks
    def create_zipstream_from_fileobj(self, files):
        pass


class test_dos_timestamp(unittest.TestCase):
    def verify_datetime(self, dt):
        dosdate, dostime = dos_timestamp(dt)
        self.assertEquals((dosdate >> 9) + 1980, dt.year)
        self.assertEquals((dosdate & 0x01f0) >> 5, dt.month)
        self.assertEquals((dosdate & 0x001f), dt.day)
        self.assertEquals((dostime >> 11), dt.hour)
        self.assertEquals((dostime & 0x07f0) >> 5, dt.minute)
        self.assertTrue(1 >= ((dostime & 0x001f)*2) - dt.second >= -1)

    def test_year(self):
        for year in range(2011, 2032):
            dt = datetime.datetime(year, 01, 01, 01, 01, 01)
            self.verify_datetime(dt)

    def test_month(self):
        for month in range(1, 13):
            dt = datetime.datetime(2011, month, 01, 01, 01, 01)
            self.verify_datetime(dt)

    def test_day(self):
        for day in range(1, 32):
            dt = datetime.datetime(2011, 01, day, 01, 01, 01)
            self.verify_datetime(dt)

    def test_hour(self):
        for hour in range(0, 24):
            dt = datetime.datetime(2011, 01, 01, hour, 01, 01)
            self.verify_datetime(dt)

    def test_minute(self):
        for minute in range(0, 60):
            dt = datetime.datetime(2011, 01, 01, 01, minute, 01)
            self.verify_datetime(dt)

    def test_second(self):
        for second in range(0, 60):
            dt = datetime.datetime(2011, 01, 01, 01, 01, second)
            self.verify_datetime(dt)

    def test_current_time(self):
        now = datetime.datetime.today()
        self.verify_datetime(now)


class test_ZipStream(ZipStreamMixin, unittest.TestCase):

    def test_consumer_implements_interface(self):
        class C(object):
            pass

        self.assertRaises(AssertionError, ZipStream, C())
        zipstream = ZipStream(TestConsumer())


    @inlineCallbacks
    def test_zip_integrity(self):
        for num_files, min_size, max_size in [(1,    0, MB(5)), 
                                              (2,    0, MB(2.5)), 
                                              (10,   0, MB(0.5)), 
                                              (100,  0, MB(0.25)),]:
                                              #(1000, 0, MB(0.001))]:
            zipstream, consumer, producers = yield self.create_zipstream_from_tempfiles(num_files, min_size=min_size, max_size=max_size)
            z = ZipFile(consumer)
            namelist = z.namelist()
            self.assertEquals(len(namelist), num_files)

            for filename, producer in producers.iteritems():
                self.assertTrue(producer.key() in namelist)
                info = z.getinfo(producer.key())
                self.assertEquals(producer.size(), info.file_size)

                with open(filename, 'r') as f:
                    self.assertEquals(binascii.crc32(f.read()) & 0xffffffff, info.CRC)


