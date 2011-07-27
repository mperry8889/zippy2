from twisted.internet import reactor
from twisted.internet.interfaces import IPushProducer
from twisted.internet.interfaces import IPullProducer
from twisted.internet.interfaces import IConsumer
from twisted.internet.defer import Deferred
from twisted.internet.defer import inlineCallbacks

from zope.interface import implements
from zope.interface import Interface

from copy import copy
from StringIO import StringIO

import datetime
import binascii
import boto
import os


class IZippyProducer(Interface):
    def beginProducing(self, consumer):
        pass

    def key(self):
        pass

    def timestamp(self):
        pass

    def size(self):
        pass

    def crc32(self):
        pass

    def generate(self):
        pass


class GeneratorBasedProducer(object):
    implements(IPushProducer)

    def __init__(self, filename):
        self.consumer = None
        self.filename = filename
        self.paused = True
        self.generator = self.generate()

    def beginProducing(self, consumer):
        self.paused = False
        self.consumer = consumer
        self.deferred = Deferred()

        self.consumer.registerProducer(self, True)
        reactor.callLater(0, self.resumeProducing)
        return self.deferred

    def pauseProducing(self):
        self.paused = True

    def resumeProducing(self):
        self.paused = False
        for block in self.generator:
            self.consumer.write(block)

            if self.paused:
                return

        # file complete
        self.consumer.unregisterProducer()
        self.deferred.callback(True)

    def stopProducing(self):
        pass


class FileProducer(GeneratorBasedProducer):
    implements(IZippyProducer)

    def __init__(self, *args, **kwargs):
        super(FileProducer, self).__init__(*args, **kwargs)
        self._timestamp = None
        self._filesize = None
        self._crc32 = None
        try:
            self._prefix = kwargs['prefix']
        except KeyError:
            self._prefix = None

    def key(self):
        if self._prefix:
            pass
        else:
            return os.path.basename(self.filename)

    def timestamp(self):
        if self._timestamp:
            return self._timestamp

        return datetime.datetime.fromtimestamp(os.path.getmtime(self.filename))

    def size(self):
        if self._filesize:
            return self._filesize

        self._filesize = os.path.getsize(self.filename)
        return self._filesize

    def crc32(self):
        if self._crc32:
            return self._crc32

        generator = self.generate()
        crc = None
        for block in generator:
            if crc:
                crc = binascii.crc32(block, crc) & 0xffffffff  # cycle
            else:
                crc = binascii.crc32(block)                    # initial crc

        self._crc32 = crc
        return crc

    def generate(self):
        with open(self.filename, 'rb') as f:
            while True:
                data = f.read(512 * 1024) # read 512KB chunks
                if not data:
                    break
                else:
                    yield data


class AwsKeyProducer(GeneratorBasedProducer):
    implements(IZippyProducer)

    def __init__(self, *args, **kwargs):
        super(AwsKeyProducer, self).__init__(*args, **kwargs)

        try:
            self.filesize = kwargs['size']
        except KeyError:
            pass

    def crc32(self):
        pass

    def generate(self):
        pass



