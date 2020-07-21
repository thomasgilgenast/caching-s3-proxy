from __future__ import print_function
import boto3
import botocore
import hashlib
import logging
from proxy.cache import LRUCache
import tempfile


class CachingS3Proxy(object):
    def __init__(self, capacity=(10*10**9), cache_dir=tempfile.gettempdir()):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.cache = LRUCache(capacity, cache_dir)
        self.s3 = boto3.resource('s3')

    def proxy_s3_bucket(self, environ, start_response):
        """proxy private s3 buckets"""
        path_info = environ.get('PATH_INFO', '')
        if path_info == '/':
            status = '200 OK'
            response_headers = [('Content-type', 'text/plain')]
            start_response(status, response_headers)
            return ['Caching S3 Proxy']

        # this used to be lstrip
        # for our purposes, a key ending in / should have the trailing /
        # stripped here; if the resulting key is a "directory" we will append
        # "/index.html" later
        path_info = path_info.strip('/')
        (bucket, key) = path_info.split('/', 1)
        try:
            s3_result = self.fetch_s3_object(bucket, key)
            status = '200 OK'
            # pip expects that index.html will be served as text/html and
            # doesn't care what we serve the actual wheel as
            response_headers = [('Content-type', 'text/html')]
        except botocore.exceptions.ClientError as ce:
            s3_result = bytes(ce.response['Error']['Message'], 'UTF-8')
            status = '404 NOT FOUND'
            response_headers = [('Content-type', 'text/plain')]

        start_response(status, response_headers)
        return [s3_result]

    def fetch_s3_object(self, bucket, key):
        m = hashlib.md5()
        m.update((bucket+key).encode('utf-8'))
        cache_key = m.hexdigest()

        try:
            return self.cache[cache_key]
        except KeyError:
            self.logger.debug('cache miss for %s' % cache_key)
            # if a key doesn't exist, append "/index.html" and try again
            try:
                obj = self.s3.Object(bucket, key).get()
            except self.s3.meta.client.exceptions.NoSuchKey:
                obj = self.s3.Object(bucket, key + '/index.html').get()
            body = obj['Body'].read()
            self.cache[cache_key] = body
            return body
