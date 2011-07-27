from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredLock
from twisted.internet.defer import inlineCallbacks
from twisted.internet.defer import returnValue
from twisted.internet.interfaces import IConsumer
from twisted.internet.interfaces import IProducer

from zippy2.producers import IZippyProducer

import datetime
import struct
import os



def dos_timestamp(timestamp):
    date = (timestamp.year - 1980) << 9 | timestamp.month << 5 | timestamp.day
    time = timestamp.hour << 11 | timestamp.minute << 5 | (timestamp.second // 2)
    return (date, time)



class ZipStream(object):

    def __init__(self, consumer):
        self.consumer = consumer
        assert IConsumer.implementedBy(consumer.__class__)

        self._producers = []

        self._sendingLock = DeferredLock()
        self._localHeaderLength = 0
        self._centralDirectoryLength = 0


    @inlineCallbacks
    def addProducer(self, producer):
        assert IZippyProducer.implementedBy(producer.__class__)
        assert IProducer.implementedBy(producer.__class__)

        yield self._sendingLock.acquire()

        self._producers.append((producer, self._localHeaderLength))

        # local file header
        timestamp = dos_timestamp(producer.timestamp())
        localHeader = struct.pack('<L5H3L2H', # format 
                                  0x04034b50, # magic (4 bytes)
                                  20, # version needed to extract (2 bytes)
                                  0, # general purpose bit flag (2 bytes)
                                  0, # compression method (2 bytes)
                                  timestamp[1], # last mod file time (2 bytes)
                                  timestamp[0], # last mod file date (2 bytes)
                                  producer.crc32() & 0xffffffff, # CRC (4 bytes)
                                  producer.size(), # compressed size (4 bytes)
                                  producer.size(), # uncompressed size (4 bytes)
                                  len(producer.key()), # file name length (2 bytes)
                                  0, # extra field length (2 bytes)
                                 )

        localHeader += producer.key()
        self.consumer.write(localHeader)
        self._localHeaderLength += len(localHeader) + producer.size()

        # file data
        yield producer.beginProducing(self.consumer)

        self._sendingLock.release()


    @inlineCallbacks
    def centralDirectory(self):
        yield self._sendingLock.acquire()

        # file header
        for producer, offset in self._producers:
            timestamp = dos_timestamp(producer.timestamp())
            fileHeader = struct.pack('<L6H3L5H2L', # format
                                     0x02014b50, # magic (4 bytes)
                                     20, # version made by (2 bytes)
                                     20, # version needed to extract (2 bytes)
                                     0, # general purpose bit flag (2 bytes)
                                     0, # compression method (2 bytes)
                                     timestamp[1], # last mod file time (2 bytes)
                                     timestamp[0], # last mod file date (2 bytes)
                                     producer.crc32() & 0xffffffff, # CRC (4 bytes)
                                     producer.size(), # compressed size (4 bytes)
                                     producer.size(), # uncompressed size(4 bytes)
                                     len(producer.key()), # file name length (2 bytes)
                                     0, # extra field length (2 bytes)
                                     0, # file comment length (2 bytes)
                                     0, # disk number start (2 bytes)
                                     0, # internal file attributes (2 bytes)
                                     0, # external file attributes (4 bytes)
                                     offset, # relative offset of local header (4 bytes)
                                    )

            fileHeader += producer.key()
            self._centralDirectoryLength += len(fileHeader)
            self.consumer.write(fileHeader)


        # end of central directory header
        endHeader = struct.pack('<L4H2LH', # format
                                0x06054b50, # magic (4 bytes)
                                0, # disk number (2 bytes)
                                0, # disk number with start of central directory (2 bytes)
                                len(self._producers), # total central directory entries on this disk (2 bytes)
                                len(self._producers), # total central directory entries (2 bytes)
                                self._centralDirectoryLength, # size of central directory (4 bytes)
                                self._localHeaderLength, # offset of start of central directory with respect to the starting disk number (4 bytes)
                                0, # zip file comment length (2 bytes)
                               )
        self.consumer.write(endHeader)



        self._sendingLock.release()


