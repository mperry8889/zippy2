from twisted.trial import unittest
from zope.interface import implements
from twisted.internet.interfaces import IConsumer
from twisted.internet.defer import inlineCallbacks
from twisted.internet.defer import DeferredList
from txaws.s3.client import S3Client

from zippy2.request import ZipRequest
from zippy2.request import ZipInvalidJson

from StringIO import StringIO

import tempfile
import random
import binascii
import os
import simplejson as json


class test_ZipRequest(unittest.TestCase):

    pass

class test_ZipRequest_from_json(unittest.TestCase):

    def test_invalid_json(self):
        self.assertRaises(Exception, ZipRequest.fromJson, 'sadf')

    def test_dummy_bucket_and_keys(self):
        json_obj = [
            { 'bucket': 'zippy2-dev', 'keys': [ 'foo.bar', 'baz.bat' ], }
        ]
        request = ZipRequest.fromJson(json_obj)
        self.assertEquals(len(request.objects), 2)


class test_ZipRequest_validation(unittest.TestCase):

    def test_missing_keys(self):
        def cb(value):
            self.fail()
        def eb(failure):
            return True

        json_obj = [
            { 'bucket': 'zippy2-dev', 'keys': [ 'asdfasdfasfasdfasdfasdf', 'asdfasdfasfasfd', 'asdfsadf' ], }
        ]
        request = ZipRequest.fromJson(json_obj)
        deferred = request.validate()
        deferred.addCallback(cb)
        deferred.addErrback(eb)
        return deferred


