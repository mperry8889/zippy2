#!/usr/bin/env python

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.python import usage
from txaws.s3.client import S3Client
from twisted.internet.defer import DeferredSemaphore

from zippy2.producers import AwsProducer
from zippy2.consumers import Crc32Consumer

import sys


class Options(usage.Options):
    optParameters = [
        ['bucket', 'b', None, 'Bucket'],
    ]


@inlineCallbacks
def main():
    options = Options()
    try:
        options.parseOptions()
    except usage.UsageError, error:
        print '%s' % error
        sys.exit(1)

    client = S3Client()
    bucket = yield client.get_bucket(options['bucket'])
    for item in bucket.contents:
        print 'Getting CRC for %s' % item.key
        producer = AwsProducer(options['bucket'], item.key)
        consumer = Crc32Consumer()
        yield producer.beginProducing(consumer)

        crc32 = '%x' % consumer.crc32()
        print '%s: %s' % (item.key, crc32)

        print 'Copying metadata to %s' % item.key
        yield client.copy_object(options['bucket'], item.key, metadata={ 'crc32': crc32 }, amz_headers={ 'storage-class': 'REDUCED_REDUNDANCY', 'metadata-directive': 'REPLACE' })


    reactor.stop()



if __name__ == '__main__':
    reactor.callLater(0, main)
    reactor.run()

