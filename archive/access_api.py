"""
softare supporting API access to a scimma data store.

This is a small librare  meant for AWS-enabled turused internal
users. and as a shim for trusted co-developers.

"""
import bson
import logging
from typing import Optional, List
from . import database_api
from . import decision_api
from . import store_api
from . import utility_api

###########
# storage 
###########

def add_parser_options(parser):
    database_api.add_parser_options(parser)
    decision_api.add_parser_options(parser)
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
        self.decider = decision_api.Decider(config)
        self.read_only = config.get("read_only", False)
    
    async def connect(self):
        await self.db.connect()
        await self.store.connect()
        if self.read_only:
            await self.db.set_read_only()
            await self.store.set_read_only()
        pass

    async def close(self):
        await self.db.close()
        await self.store.close()
        pass
    
    async def get_metadata(self, uuid):
        "get metadata, if any associated with a message with the given ID"
        return await self.db.fetch(uuid)
    
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
        message = bundle["message"]
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
        annotations = self.decider.get_annotations(payload, metadata.headers,
                                                   public=public, direct_upload=direct_upload)
        if await self.decider.is_deemed_duplicate(annotations, metadata, self.db, self.store):
            logging.info(f"Duplicate not stored {annotations}")
            return (False,{},"Message with duplicate UUID not stored")
        text_to_index = self.decider.get_indexable_text(payload, metadata.headers, annotations)
        await self.store.store(payload, metadata, annotations)
        await self.db.insert(metadata, annotations, text_to_index)
        return (True,
                {"archive_uuid": annotations["con_text_uuid"],
                 "is_client_uuid": annotations["con_is_client_uuid"]},
                "")

    async def get_topics_with_public_messages(self):
        """
        Get the names of all topics on which at least one public message is archived
        """
        return await self.db.get_topics_with_public_messages()
    
    async def get_message_records(self, *args, **kwargs):
        """
        Get the records for messages satisfying specified criteria, with results split/batched into
        'pages'.
        Selecting messages by topic has some complexity: First, if neither topics_public nor
        topics_full is specified, the default is to select public messages from any topic. If either
        topic restriction argument is specified, no message is returned which is on a topic not
        specified by one of the two arguments. Both arguments may be specified at the same time to
        select a union of messages across multiple topics with different access levels.
        
        Args:
            bookmark: If not None, this must be a 'bookmark' string returned by a previous call, to
                      select another page of results.
            page_size: The maximum number of results to return fro this call; any further results
                       can be retrieved as subsequent 'pages'. 
            ascending: Whether the reuslts should be sorted in ascending timestamp order.
            topics_public: If not None, only messages which are flagged as being public appearing on
                           these topics will be returned. Can be used at the same time as
                           topics_full.
            topics_full: If not None, any message appearing on one of these topics is a cadidate to
                         be returned.
            start_time: The beginning of the message timestamp range to select.
            end_time: The end of the message timestamp range to select. The range is half-open, so
                      messages with this exact timestamp will be excluded.
        Return: A tuple consisting of the results (a list of MessageRecords), a 'bookmark' which can
                be used to fetch the next page of results or None if there are no subsequent
                results, and a 'bookmark' for the previous page of results or None if there are no
                prior results.
        """
        return await self.db.get_message_records(*args, **kwargs)
    
    async def count_message_records(self, *args, **kwargs):
        """
        Count the numberof messages satisfying specified criteria, as they would be returned by
        get_message_records.
        
        Args:
            topics_public: If not None, only messages which are flagged as being public appearing on
                           these topics will be returned. Can be used at the same time as
                           topics_full.
            topics_full: If not None, any message appearing on one of these topics is a cadidate to
                         be returned.
            start_time: The beginning of the message timestamp range to select.
            end_time: The end of the message timestamp range to select. The range is half-open, so
                      messages with this exact timestamp will be excluded.
        Return: An integer count of messages
        """
        return await self.db.count_message_records(*args, **kwargs)
    
    async def search_message_text(self, *args, **kwargs):
        return await self.db.search_message_text(*args, **kwargs)
    
    async def count_text_search_results(self, *args, **kwargs):
        return await self.db.count_text_search_results(*args, **kwargs)
