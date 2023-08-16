"""
softare supporting API access to a scimma data store.

This is a small librare  meant for AWS-enabled turused internal
users. and as a shim for trusted co-developers.

"""
import bson
from . import database_api
from . import decision_api
from . import store_api

###########
# storage 
###########

class Archive_access():
    def __init__(self, config, read_only: bool = False):
        """
        Instantiate DB objects and store objects

        """
            
        self.db     = database_api.DbFactory(config)
        self.store  = store_api.StoreFactory(config)
        self.db.connect()
        self.store.connect()
        if read_only:
            self.db.set_read_only()
            self.store.set_read_only()
    
    def get_raw_object(self, key):
        "get the raw object -- bson bundle)"
        bundle = self.store.get_object(key)
        return bundle

    def get_all(self, key):
        "get message and all retained metadata (includes header)"
        bundle = self.get_raw_object(key)
        bundle =  bson.loads(bundle)
        message = bundle["message"]["content"]
        metadata = bundle["metadata"]
        return (message, metadata)

    def get_as_sent(self, key):
        "get message and header as sent to kafka"
        message, metadata =  self.get_all(key)
        return (message, metadata["headers"])

    def get_object_summary(self, key):
        "check if object is present, and return size if so"
        return self.store_get_object_summary()

    def store_message(self, payload, metadata):
        annotations = decision_api.get_annotations(payload, metadata.headers)
        if decision_api.is_deemed_duplicate(annotations, metadata, self.db, self.store):
            logging.info(f"Duplicate not stored {annotations}")
            return False
        self.store.store(payload, metadata, annotations)
        self.db.insert(payload, metadata, annotations)
        return True