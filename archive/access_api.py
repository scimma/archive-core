"""
softare supporting API access to a scimma data store.

This is a small librare  meant for AWS-enabled turused internal
users. and as a shim for trusted co-developers.

"""
import bson
import logging
from . import database_api
from . import decision_api
from . import store_api
from . import utility_api

###########
# storage 
###########

def add_parser_options(parser):
    database_api.add_parser_options(parser)
    store_api.add_parser_options(parser)
    parser.add_argument("--read-only", help="Set whether only read operations are permitted",
                        type=bool, action=utility_api.EnvDefault, envvar="READ_ONLY",
                        required=False, default=False)

class Archive_access():
    def __init__(self, config):
        """
        Instantiate DB objects and store objects

        """
        self.db     = database_api.DbFactory(config)
        self.store  = store_api.StoreFactory(config)
        self.read_only = config.get("read_only", False)
    
    async def connect(self):
        await self.db.connect()
        await self.store.connect()
        if self.read_only:
            await self.db.set_read_only()
            await self.store.set_read_only()

    async def close(self):
        await self.db.close()
        await self.store.close()
    
    async def get_metadata(self, uuid):
        "get metadata, if any associated with a message with the given ID"
        return await self.db.fetch(uuid)
    
    async def get_metadata_for_time_range(self, topic: str, start_time: int, end_time: int, limit: int=10, offset: int=0):
        "get metadata, if any associated with a message with the given ID"
        return await self.db.get_message_records_for_time_range(topic, start_time, end_time, limit, offset)
    
    async def get_object_lazily(self, key):
        """
        get the raw object in the form of the S3 response which can be streamed
        """
        return await self.store.get_object_lazily(key)
    
    async def get_raw_object(self, key):
        "get the raw object -- bson bundle"
        bundle = await self.store.get_object(key)
        return bundle

    async def get_all(self, key):
        "get message and all retained metadata (includes header)"
        bundle = await self.get_raw_object(key)
        if bundle is None:
            return None
        bundle =  bson.loads(bundle)
        message = bundle["message"]["content"]
        metadata = bundle["metadata"]
        return (message, metadata)

    async def get_as_sent(self, key):
        "get message and header as sent to kafka"
        record = await self.get_all(key)
        if record is None:
            return record
        message, metadata = record
        return (message, metadata["headers"])

    async def get_object_summary(self, key):
        "check if object is present, and return size if so"
        return await self.store.get_object_summary(key)

    async def store_message(self, payload, metadata, public: bool=True, direct_upload: bool=False):
        annotations = decision_api.get_annotations(payload, metadata.headers,
                                                   public=public, direct_upload=direct_upload)
        if await decision_api.is_deemed_duplicate(annotations, metadata, self.db, self.store):
            logging.info(f"Duplicate not stored {annotations}")
            return (False,"Message with duplicate UUID not stored")
        await self.store.store(payload, metadata, annotations)
        await self.db.insert(metadata, annotations)
        return (True,"")