from twisted.application.service import IServiceMaker
from twisted.plugin import IPlugin
from twisted.python import usage

from zope.interface import implements

from zippy2.service import StreamingServiceApi


class Options(usage.Options):
    optParameters = [
        ['port', 'p', '11000', 'Port'],
        ['base-url', 'u', '', 'Base URL'],
    ]


class StreamingServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    tapname = 'z2-streaming'
    description = 'Zippy2 Streaming Server'
    options = Options

    def makeService(self, options):
        service = StreamingServiceApi()
        return service


serviceMaker = StreamingServiceMaker()

