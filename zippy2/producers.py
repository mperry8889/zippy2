from twisted.internet import reactor
from twisted.internet.interfaces import IPushProducer
from twisted.internet.defer import Deferred
from twisted.internet.defer import returnValue
from twisted.internet.defer import inlineCallbacks
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from twisted.internet.protocol import Protocol

from txaws.s3.client import Query as txAwsQuery
from txaws.s3.client import URLContext 
from txaws.s3.client import S3Client

from zope.interface import implements
from zope.interface import Interface

import datetime
import binascii
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

    def verify(self):
        pass


class GeneratorBasedProducer(object):
    implements(IPushProducer)

    def __init__(self):
        self.consumer = None
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

    def __init__(self, filename, *args, **kwargs):
        super(FileProducer, self).__init__(*args, **kwargs)
        self.filename = filename
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

    def verify(self):
        return True

    def generate(self):
        with open(self.filename, 'rb') as f:
            while True:
                data = f.read(512 * 1024) # read 512KB chunks
                if not data:
                    break
                else:
                    yield data



# Use Twisted's new Agent HTTP client instead of the usual HTTPClient
# that txAws uses.  This is required for streaming of S3 objects
# since HTTPClient wants to just put the data in memory and return it
# via a callback

class txAwsAgentQuery(txAwsQuery):
    def submit(self, url_context=None):
        if not url_context:
            url_context = URLContext(
                self.endpoint, self.bucket, self.object_name)
        agent = Agent(reactor)
        headers = Headers()
        for k, v in self.get_headers().iteritems():
            headers.addRawHeader(k, v)
        return agent.request(self.action, url_context.get_url(), headers)


class AwsProducerMissingCRC(Exception): pass
class AwsProducerKeyError(Exception): pass


class AwsProducer(Protocol):
    implements(IZippyProducer)

    def __init__(self, bucket, key, *args, **kwargs):
        self._key = key
        self._bucket = bucket
        self._s3headers = None

        self._s3client = S3Client()
        self._s3agent = S3Client(query_factory=txAwsAgentQuery)

        self._stopped = False

    @inlineCallbacks
    def get_headers(self):
        self._s3headers = yield self._s3client.head_object(self._bucket, self._key)

    def key(self):
        return self._key

    @inlineCallbacks
    def timestamp(self):
        if not self._s3headers:
            yield self.get_headers()

        returnValue(datetime.datetime.strptime(self._s3headers['last-modified'][0], '%a, %d %b %Y %H:%M:%S %Z'))

    @inlineCallbacks
    def size(self):
        if not self._s3headers:
            yield self.get_headers()
        returnValue(int(self._s3headers['content-length'][0]))
        
    @inlineCallbacks
    def crc32(self):
        if not self._s3headers:
            yield self.get_headers()

        returnValue(int(self._s3headers['x-amz-meta-crc32'][0], 16))

    @inlineCallbacks
    def verify(self):
        if not self._s3headers:
            try:
                yield self.get_headers()
            except Exception, e:
                # 404 not found
                raise AwsProducerKeyError('Key not found')

        try:
            crc32 = yield self.crc32()
        except KeyError:
            raise AwsProducerMissingCRC('CRC32 metadata missing from key')


    def dataReceived(self, data):
        self.consumer.write(data)

    def connectionLost(self, reason):
        self.stopProducing()

    @inlineCallbacks
    def beginProducing(self, consumer):
        self.consumer = consumer
        self.deferred = Deferred()

        self.consumer.registerProducer(self, True)
        self._response = yield self._s3agent.get_object(self._bucket, self._key)
        self._response.deliverBody(self)

        yield self.deferred

    def pauseProducing(self):
        self.transport.pauseProducing()

    def resumeProducing(self):
        self.transport.resumeProducing()

    def stopProducing(self):
        if not self._stopped:
            self.consumer.unregisterProducer()
            self.deferred.callback(True)
            self._stopped = True

