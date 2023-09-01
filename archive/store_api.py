"""
Provide classes and API to stores  of archive objects.

There are two types homeomorphic classes

One class accesses AWS S3. This class can be configured
via archive_ingest.toml. It can access the production or
development S3 buckets via different configurations.

The other class  is "mock" store useful for
development and test. This class  discards the data.

The StoreFactory class supports choosing which class is
used at run-time.

All classes use a namespace object (config), such
as provided by argparse, as part of their interface.
"""

import boto3
import botocore
import aioboto3
import bson
import logging
import time
from . import utility_api
import zlib
import os

##################################
# stores
##################################

def StoreFactory(config):
    type = config["store_type"]
    #instantiate, then return db object of correct type.
    if type == "mock" : return Mock_store(config)
    if type == "S3"   : return S3_store  (config)
    raise RuntimeError(f"store {type} not supported")


def add_parser_options(parser):
    EnvDefault=utility_api.EnvDefault
    parser.add_argument("--store-type", help="Type of the main data storage", type=str, choices=["S3", "mock"], action=EnvDefault, envvar="STORE_TYPE", default="S3")
    parser.add_argument("--store-primary-bucket", help="Name of the main object store bucket", type=str, action=EnvDefault, envvar="STORE_PRIMARY_BUCKET", default="hopskotch-archive")
    parser.add_argument("--store-backup-bucket", help="Name of the backup object store bucket", type=str, action=EnvDefault, envvar="STORE_BACKUP_BUCKET", default="hopskotch-archive-backup")
    parser.add_argument("--store-endpoint-url", help="URL for the object store", type=str, action=EnvDefault, envvar="STORE_ENDPOINT_URL", required=False)
    parser.add_argument("--store-region-name", help="Name of the object store region", type=str, action=EnvDefault, envvar="STORE_REGION_NAME", required=False)
    parser.add_argument("--store-log-every", help="Frequency of object store logging", type=int, action=EnvDefault, envvar="STORE_LOG_EVERY", default=100, required=False)


class Base_store:
    " base class for common methods"
    def __init__(self, config):
        self.primary_bucket = config["store_primary_bucket"]
        self.backup_bucket  = config["store_backup_bucket"]
        ## If custom S3 endpoint is specified, assume non-AWS config
        if 'store_endpoint_url' in config and config['store_endpoint_url'] is not None:
            self.s3_provider = 'custom'
            self.s3_endpoint_url = config['store_endpoint_url']
            self.s3_region_name = config['store_region_name']
            self.aws_access_key_id=os.environ['S3_ACCESS_KEY_ID']
            self.aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY']
        else:
            self.s3_provider = 'aws'
            self.s3_endpoint_url = ''
            self.s3_region_name = ''
            self.aws_access_key_id= ''
            self.aws_secret_access_key= ''
        self.n_stored = 0
        self.log_every = config.get("store_log_every",100)
        self.read_only = False
        self.config = config

    def log(self, annotations):
        "log storage informmation, but not too often"
        msg1 = f"stored {self.n_stored} objects."
        msg2 = f"This object: {annotations['size']} bytes to {annotations['bucket']} {annotations['key']}"
        if self.n_stored < 5 or self.n_stored % self.log_every == 0:
            logging.info(msg1)
            logging.info(msg2)

    async def set_read_only(self):
        """
        Configure this stroage object to only perform reads, rejecting all
        modification operations.
        """
        self.read_only = True

    def get_key(self, metadata, text_uuid):
        'compute the "path" to the object'
        topic = metadata.topic
        t = time.gmtime(metadata.timestamp/1000)
        key = f"{topic}/{t.tm_year}/{t.tm_mon}/{t.tm_mday}/{t.tm_hour}/{text_uuid}.bson"
        return key

    def set_storeinfo(self, annotations, key, size, crc32):
        annotations['size'] = size
        annotations['key'] = key
        annotations['bucket'] = self.primary_bucket
        annotations['crc32'] = crc32

    def get_as_bson(self, payload, metadata, annotations):
        "return a blob of bson"
        simplified_metadata = {"timestamp" : metadata.timestamp,
                               "headers" : metadata.headers,
                               "topic" : metadata.topic
                              }
        ret = bson.dumps({"message" : payload,
                          "metadata" : simplified_metadata,
                          "annotations": annotations
                          })
        return ret

    def get_object(self, key):
        "if not overriden, print error and die"
        raise NotImplementedError

    def get_object_summary(self, key):
        "if not overriden, print error and die"
        raise NotImplementedError

    def deep_delete_object_from_store(self, key):
        "if not overriden, print error and die"
        raise NotImplementedError


class S3_store(Base_store):
    "send things to s3"
    def __init__(self, config):
        super().__init__(config)

    async def connect(self):
        "obtain an S3 Client"
        if self.s3_provider == 'custom':
            self.client = await aioboto3.Session().client('s3',
                endpoint_url=self.s3_endpoint_url,
                region_name = self.s3_region_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            ).__aenter__()
            await self.initialize_bucket(bucket=self.primary_bucket)
            await self.initialize_bucket(bucket=self.backup_bucket)
        else:
            self.client = await aioboto3.Session().client('s3').__aenter__()

    async def close(self):
        await self.client.close()

    async def initialize_bucket(self, bucket=''):
        exists = True
        try:
            await self.client.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = e.response['Error']['Code']
            if error_code == '404':
                exists = False
        if not exists:
            ## Create buckets if they do not exist
            await self.client.create_bucket(Bucket=bucket)

    async def store(self, payload, metadata, annotations):
        """place data, metadata as an object in S3"""
        if self.read_only:
            raise RuntimeError("This store object is set to read-only; store is forbidden")

        bucket = self.primary_bucket
        key = self.get_key(metadata, annotations["con_text_uuid"])
        b = self.get_as_bson(payload, metadata, annotations)
        size = len(b)
        crc32 = zlib.crc32(b)
        await self.client.put_object(Body=b, Bucket=bucket, Key=key)
        self.n_stored += 1
        self.set_storeinfo(annotations, key, size, crc32)
        self.log(annotations)
        return

# TODO: These implementations do not work. Deletion is also a rare task, so fixing them is deferred.
#     async def deep_delete_object_from_store(self, key):
#         """
#         delete all corresponding objects  from all S3 archive
#         including versions and delete markers.
#        """
#         if self.read_only:
#             raise RuntimeError("This store object is set to read-only; "
#                                "deep_delete_object_from_store is forbidden")
#         await self.deep_delete_object(self.primary_bucket, key)
#         await self.deep_delete_object(self.backup_bucket, key)
# 
#     async def deep_delete_object(self, bucket_name, key):
#         """
#         delete all contents related to object from the S3
#         bucket.
#         """
#         if self.read_only:
#             raise RuntimeError("This store object is set to read-only; "
#                                "deep_delete_object is forbidden")
# 
#         bucket = await self.client.Bucket(bucket_name)
#         await bucket.object_versions.filter(Prefix=key).delete()

    async def get_object_lazily(self, key):
        """
        Fetch an object from S3, directly returning the S3 response object,
        allowing uses like streaming the object data.
        """
        try:
            response = await self.client.get_object(
                Bucket=self.primary_bucket,
                Key=key)
            return response
        except botocore.errorfactory.ClientError as err:
            if "NoSuchKey" in err.__repr__() : return None
            raise

    async def get_object(self, key):
        "return oject from S3"
        try:
            response = await self.client.get_object(
                Bucket=self.primary_bucket,
                Key=key)
        except botocore.errorfactory.ClientError as err:
            if "NoSuchKey" in err.__repr__() : return None
            raise
        data = await response['Body'].read()
        return data

    async def get_object_summary(self, key):
        "if not overriden, print error and die"
        summary = {"exists" : False}
        try:
            response = await self.client.get_object(
                Bucket=self.primary_bucket,
                Key=key)

        except botocore.errorfactory.ClientError as err:
            # Its important to differentiate between this
            # error and any other BOTO error.
            #
            # oh my! if the key does not exists,
            # boto throws a dymanically made class
            # botocore.errorfactory.NoSuchKey. Because the
            # class is dynamically made, it can't be
            # used in the except statement, above. so I've
            # provided this hokey test against __repr__
            # to indicate the key does not  exist.

            if "NoSuchKey" in err.__repr__() : return summary
            raise
        summary = {"exists" : True}
        size  = response["ContentLength"]
        summary["size"] = size
        return summary

# TODO: implementation incomplete    
#     async def list_object_versions(self, prefix):
#         """ list all onecht verision under prefix"""
#         my_bucket = await self.client.Bucket('self.primary_bucket')
#         async for object in my_bucket.objects.all():
#             print(object.key)
#         """    
#         paginator = client.get_paginator('list_objects')
#         result = paginator.paginate(Bucket=self.primary_bucket
#                                     , Delimiter=prefix)
#         for prefix in result.search('CommonPrefixes'):
#             print(prefix.get('Prefix'))
#         return
#         objectXSXCs = list(bucket.objects.filter(Prefix=prefix))
#         for object in objects:
#             for result in self.client.list_object_versions(
#                 Bucket=self.primary_bucket,
#                 Prefix=path_prefix):
#             yield result
#         """

    
class Mock_store(Base_store):
    """
    a mock store that does nothing -- support debug and devel.
    """

    def __init__(self, config):
        super().__init__(config)
        self.buckets = {}
        self.connected = False
        logging.info(f"Mock store configured")

    async def connect(self):
        self.connected = True

    async def close(self):
        self.connected = False

    async def store(self, payload, metadata, annotations):
        """place data, metadata as an object in S3"""
        if self.read_only:
            raise RuntimeError("This store object is set to read-only; store is forbidden")
        assert self.connected

        bucket = self.primary_bucket
        if not bucket in self.buckets:
            self.buckets[bucket] = {}
        
        key = self.get_key(metadata, annotations["con_text_uuid"])
        b = self.get_as_bson(payload, metadata, annotations)
        size = len(b)
        crc32 = zlib.crc32(b)
        self.buckets[bucket][key] = b
        
        self.n_stored += 1
        self.set_storeinfo(annotations, key, size, crc32)
        self.log(annotations)

    async def deep_delete_object_from_store(self, key):
        if self.read_only:
            raise RuntimeError("This store object is set to read-only; "
                               "deep_delete_object_from_store is forbidden")
        assert self.connected
        await self.deep_delete_object(self.primary_bucket, key)
        await self.deep_delete_object(self.backup_bucket, key)

    async def deep_delete_object(self, bucket, key):
        if self.read_only:
            raise RuntimeError("This store object is set to read-only; "
                               "deep_delete_object is forbidden")
        assert self.connected
        if bucket in self.buckets and key in self.buckets[bucket]:
            del self.buckets[bucket][key]

    async def get_object(self, key):
        assert self.connected
        if key in self.buckets[self.primary_bucket]:
            return self.buckets[self.primary_bucket][key]
        return None
    
    class LazyObject:
        def __init__(self, obj):
            self.obj = obj
        def read(self):
            return self.obj

    async def get_object_lazily(self, key):
        assert self.connected
        if key in self.buckets[self.primary_bucket]:
            return self.LazyObject(self.buckets[self.primary_bucket][key])
        return None

    async def get_object_summary(self, key):
        assert self.connected
        summary = {"exists" : False}
        if self.primary_bucket in self.buckets and key in self.buckets[self.primary_bucket]:
            summary["exists"] = True
            summary["size"] = len(self.buckets[self.primary_bucket][key])
        return summary
