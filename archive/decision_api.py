"""
Support Poicy decsions about dupliate messages.

Duplicate messages arise from more-than-once
delivery or a kafka cursor reset. The module is
also a place holded for future decision logic.

The modle trusts information in the Database.
The module does not consult S3, which has a
separate disater recovery capability.

This moodule supports
- in-line decisions integrated into
  message acquisitions.
- decisions  intergrated into
  archive-oriented tools
"""

#################
# Batch decisions routines
#################

import logging
import uuid
import zlib


def is_content_identical(ids, db, store):
    "ensure that messages have the same content"
    import zlib
    import bson
    contents = []
    locations = db.get_message_locations(ids)
    for bucket, key in locations:
        content = bson.loads(store.get(key))
        contents.append(content)
    crc_set = {zlib.crc32(c["message"]["content"]) for c in contents}
    return len(crc_set) == 1


def get_text_uuid(headers):
	"""
	Determine the UUID for a message.
	If a (valid) UUID is included as a message header, use that, 
	otherwise, assign a new UUID. 
	
	Return: A tuple of the UUID as a string and a flag indicating
	        whether it was one originally provided by the client.
	"""

	text_uuid = ""
	_ids = [item for item in headers if item[0] == "_id"] if headers else []
	#return the first valid uuid.
	for _id in _ids:
		try:
			binary_uuid = _id[1]
			return (str(uuid.UUID(bytes=binary_uuid)), True)
		except (ValueError, IndexError, TypeError) :
			continue
	#nothing there; so make one up.
	text_uuid = str(uuid.uuid4())
	logging.debug(f"I made a UUID up: {text_uuid}")
	return (text_uuid, False)
    

def get_annotations(message, headers=[]):
	"""
	Assign standard annotations to a message based on its contents and any headers
	
	Return: A dictionary of annotations
	"""
	annotations = {}
	text_uuid, is_client_uuid  = get_text_uuid(headers)
	annotations["con_text_uuid"] = text_uuid
	annotations["con_is_client_uuid"] = is_client_uuid
	annotations["con_message_crc32"] =  zlib.crc32(message["content"])
	return annotations


###################
# routines to decide if a message is idenitcal to one ...
# already in the archive.
###################


async def is_deemed_duplicate(annotations, metadata, db, store):
    """
    Decide if this message is a duplicate.
    Updates annotations with a duplicate entry mapping to the result
    """
    # storage decision_data
    if annotations['con_is_client_uuid']:
        # Use the fact that client side UUID are unique
        duplicate = await db.uuid_in_db(annotations['con_text_uuid'])
    else:
        # server side UUID.
        message_crc32 = annotations["con_message_crc32"]
        duplicate = await db.exists_in_db(metadata.topic, metadata.timestamp, message_crc32)
    annotations["duplicate"] = duplicate
    return duplicate