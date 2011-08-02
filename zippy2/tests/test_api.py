from twisted.trial import unittest
from zope.interface import implements
from twisted.internet.interfaces import IConsumer
from twisted.internet.defer import inlineCallbacks
from twisted.internet.defer import DeferredList
from txaws.s3.client import S3Client

from zippy2.producers import FileProducer
from zippy2.producers import AwsProducer

from StringIO import StringIO

import tempfile
import random
import binascii
import os

