from twisted.internet import reactor
from twisted.internet.interfaces import IPushProducer
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import inlineCallbacks
from twisted.internet.defer import returnValue

from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.resource import Resource

from zippy2.request import ZipRequest
from zippy2.stream import ZipStream

import simplejson as json


class Create(Resource):

    def render_response(self, value, zip_request, request):
        print '%s.zip' % zip_request.id
        ZipRoot.putChild('%s.zip' % zip_request.id, ZipNode(zip_request))
        request.write('/get/%s.zip' % zip_request.id)
        request.finish()

    def render_error(self, error, request):
        request.setResponseCode(401)
        request.write('NO')
        request.finish()

    def render_POST(self, request):
        json_request = json.loads(request.content.read())
        zip_request = ZipRequest.fromJson(json_request)
        deferred = zip_request.validate()
        deferred.addCallback(self.render_response, zip_request, request)
        deferred.addErrback(self.render_error, request)
        return NOT_DONE_YET


class ZipNode(Resource):
    isLeaf = True

    def __init__(self, zip_request, *args, **kwargs):
        Resource.__init__(self, *args, **kwargs)
        self.zip_request = zip_request


    def render_GET(self, request):
        request.setHeader('Content-Type', 'application/zip')
        deferred = self.zip_request.render(request)
        deferred.addCallback(lambda _: request.finish())
        return NOT_DONE_YET



ApiRoot = Resource()
ApiRoot.putChild('create', Create())

ZipRoot = Resource()
ApiRoot.putChild('get', ZipRoot)

