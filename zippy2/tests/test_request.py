from twisted.trial import unittest

from zippy2.request import ZipRequest


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


