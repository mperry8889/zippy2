from twisted.internet import reactor
from twisted.internet.interfaces import IPushProducer
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import inlineCallbacks
from twisted.internet.defer import returnValue
from twisted.web.resource import Resource

from zippy2.producers import AwsProducer
from zippy2.stream import ZipStream

import uuid

class ZipRequestError(Exception):
    pass

class ZipInvalidJson(Exception):
    pass

class ZipInvalidRequest(Exception):
    pass



class ZipRequest(object):
    def __init__(self, objects):
        self.objects = objects
        self.producers = []
        self.id = str(uuid.uuid4())


    def validate(self):
        deferreds = []

        for bucket, key in self.objects:
            producer = AwsProducer(bucket, key)
            self.producers.append(producer)
            deferreds.append(producer.verify())

        deferredlist = DeferredList(deferreds, fireOnOneErrback=True, consumeErrors=True)
        return deferredlist


    def render(self, request):
        if not self.producers:
            for bucket, key in self.objects:
                producer = AwsProducer(bucket, key)
                self.producers.append(producer)
        
        zipstream = ZipStream(request)
        deferreds = []

        for producer in self.producers:
            d = zipstream.addProducer(producer)
            deferreds.append(d)

        deferredlist = DeferredList(deferreds)
        deferredlist.addCallback(lambda _: zipstream.centralDirectory())
        return deferredlist


    @classmethod
    def fromJson(cls, json_object):
        objects = []

        for i in json_object:
            bucket = i['bucket']
            keys = i['keys']

            for key in keys:
                objects.append((bucket, key))

        return ZipRequest(objects)




