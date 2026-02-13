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

import bson
import logging
import uuid
import zlib

import magic
import hop.models

from . import utility_api


def add_parser_options(parser):
	EnvDefault=utility_api.EnvDefault
	parser.add_argument("--text-index-message-size-limit",
	                    help="The maximum size a message can be to have text indexed from it for searching", 
	                    type=int, action=EnvDefault, envvar="TEXT_INDEX_MESSAGE_SIZE_LIMIT", default=1<<22, required=False)
	parser.add_argument("--text-index-size-limit",
	                    help="The maximum amount of text extracted from a message which will be indexed for searching", 
	                    type=int, action=EnvDefault, envvar="TEXT_INDEX_SIZE_LIMIT", default=1<<14, required=False)

async def is_content_identical(ids, db, store):
    "ensure that messages have the same content"
    contents = []
    locations = await db.get_message_locations(ids)
    for bucket, key in locations:
        content = bson.loads(await store.get_object(key))
        contents.append(content)
    crc_set = {zlib.crc32(c["message"]) for c in contents}
    return len(crc_set) == 1


class Decider:
	def __init__(self, config):
		self.magic = magic.Magic(flags=magic.MAGIC_MIME_TYPE)
		self.text_index_message_size_limit = config.get("text_index_message_size_limit", 1<<22)
		self.text_index_size_limit = config.get("text_index_size_limit", 1<<14)


	def close(self):
		self.magic.close()


	def __enter__(self):
		return self


	def __exit__(self, exc_type, exc_value, traceback):
		self.close()


	def get_text_uuid(self, headers):
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
				if not isinstance(binary_uuid, bytes):
					binary_uuid = bytes(binary_uuid)
				return (str(uuid.UUID(bytes=binary_uuid)), True)
			except (ValueError, IndexError, TypeError):
				continue
		#nothing there; so make one up.
		text_uuid = str(uuid.uuid4())
		logging.debug(f"I made a UUID up: {text_uuid}")
		return (text_uuid, False)


	def get_string_header(self, headers, target):
		"""Get the value of a specific header as a string, if it exists and can be decoded
		"""
		for header in headers:
			if header[0] == target:
				try:
					# accept the first thing we get and can decode
					return header[1].decode("utf-8")
				except:
					pass # ignore decoding errors
		return ""


	def hop_format_to_media_type(self, format_name):
		known_hop_formats = {
			b"voevent": "application/x.voevent",
			b"gcntextnotice": "application/x.gcn.notice",
			b"circular": "application/x.gcn.circular",
			b"blob": "application/octet-stream", 
			b"json": "application/json", 
			b"avro": "application/avro", # not actually valid as not registered with IANA, but people seem to use it
			b"external": "message/x.hop.external+json",
		}
		return known_hop_formats.get(format_name, None)


	def get_data_format(self, message, headers):
		format = None
		# first, if the message has a hop _format header, use that
		# note that we do not break out of the loop after finding a candidate value, 
		# so that the last value wins; we favor the last because hop-client appends to the headers
		for header in headers:
			if header[0] == "_format":
				logging.debug(f" Found hop format header: {header[1]}")
				candidate = self.hop_format_to_media_type(header[1])
				if candidate is not None:
					format = candidate
					logging.debug(f" tentative format is {format}")
			
		# appliction/octet-stream is not very specific, so we would like to do better if we can
		if format is not None and format != "application/octet-stream":
			return format
		
		# otherwise, get out the big guns and query libmagic
		max_len = 4096
		if len(message) <= max_len:
			sample = bytes(message)
		else:
			sample = bytes(message[0:max_len])
		magic_format=self.magic.id_buffer(sample)
		logging.debug(f"  libmagic says: {magic_format}")
		
		return magic_format
	
	
	def get_annotations(self, message, headers=[], public: bool=True, direct_upload: bool=False):
		"""
		Assign standard annotations to a message based on its contents and any headers
		
		Return: A dictionary of annotations
		"""
		annotations = {}
		text_uuid, is_client_uuid  = self.get_text_uuid(headers)
		annotations["con_text_uuid"] = text_uuid
		annotations["con_is_client_uuid"] = is_client_uuid
		annotations["con_message_crc32"] = message.crc32() if hasattr(message, "crc32") else zlib.crc32(message)
		annotations["public"] = public
		annotations["direct_upload"] = direct_upload
		annotations["media_type"] = self.get_data_format(message, headers)
		annotations["title"] = self.get_string_header(headers, "title")
		annotations["sender"] = self.get_string_header(headers, "_sender")
		annotations["file_name"] = self.get_string_header(headers, "file_name")
		return annotations
	
	
	###################
	# routines to decide if a message is idenitcal to one ...
	# already in the archive.
	###################
	
	
	async def is_deemed_duplicate(self, annotations, metadata, db, store):
		"""
		Decide if this message is a duplicate.
		Re-assigns the message UUID if necessary to prevent collision.
		"""
		# storage decision_data
		text_uuid = annotations['con_text_uuid']
		duplicate = await db.exists_in_db(metadata.topic, metadata.timestamp, annotations["con_message_crc32"])
		if not duplicate: # check that there is no UUID collision
			while await db.uuid_in_db(text_uuid):
				logging.info(f"UUID {text_uuid} already present in DB with different data; reassigning UUID")
				text_uuid = str(uuid.uuid4())
				annotations["con_text_uuid"] = text_uuid
				annotations["con_is_client_uuid"] = False	
		return duplicate


	def get_indexable_text(self, message, headers, annotations):
		chunks = []
		
		# try to collect text from headers
		# ignore reserved headers, whose names start with underscores, 
		# except the _sender header, which is text and of interest for searching
		for header in headers:
			if not header[0].startswith('_') or header[0]=="_sender":
				try:
					s = header[1].decode("utf-8")
				except Exception as ex:
					# headers may not be text, so treat decoding failures as unimportant
					pass
				else:
					chunks.append(header[0] if not header[0].startswith('_') else header[0][1:])
					chunks.append(s)
		
		# to prevent high memory use from unpacking various formats, only index message contents
		# if the payload is small enough
		annotations["text_fully_indexed"] = False
		if len(message) < self.text_index_message_size_limit:
			if "media_type" in annotations and annotations["media_type"] in text_extractors:
				chunks.extend(text_extractors[annotations["media_type"]](message))
				annotations["text_fully_indexed"] = True
		
		text = ' '.join(chunks)
		if len(text) > self.text_index_size_limit:
			logging.warning(f"Extracted {len(text)} bytes of text from a message, "
			                f"truncating to {self.text_index_size_limit} bytes")
			text = text[0:self.text_index_size_limit]
		return text


def should_index(s):
	"""Try to avoid indexing base64 and similar encoded data by ignoring long strings
	which contain no spaces, as these are unlikely to be human-readable text.
	"""
	if len(s) >= 128:
		return ' ' in s
	else:
		return True


def extract_text(obj, text, **kwargs):
	"""Spider through an object graph rooted at obj, accumulating text which seems indexable
	into text.
	"""
	
	if isinstance(obj, str):
		if should_index(obj):
			text.append(obj)
	elif isinstance(obj, dict):
		for key, val in obj.items():
			if isinstance(val, str) and should_index(val):
				text.append(key)
				text.append(val)
			elif isinstance(val, int) or isinstance(val, float):
				text.append(key)
				text.append(str(val))
			else:
				extract_text(val, text, **kwargs)
	# TODO: this is where we might want to inspect bytes objects to see if they can be decoded
	elif isinstance(obj, (list, tuple)):
		for item in obj:
			extract_text(item, text, **kwargs)


def extract_indexable_text_json(raw, **kwargs):
	text = []
	extract_text(hop.models.JSONBlob.deserialize(raw).content, text, **kwargs)
	return text


def extract_indexable_text_avro(raw, **kwargs):
	text = []
	extract_text(hop.models.AvroBlob.deserialize(raw).content, text, **kwargs)
	return text


def extract_indexable_text_plain(raw, **kwargs):
	try:
		decoded = raw.decode("utf-8")
		if should_index(decoded):
			return [decoded]
	except:
		# data wasn't UTF-8; ignore
		pass
	return []


def extract_indexable_text_circular(raw, **kwargs):
	decoded = hop.models.GCNCircular.deserialize(raw)
	text = []
	for title, content in decoded.header.items():
		text.append(title)
		text.append(content)
	text.append(decoded.body)
	return text


def extract_indexable_text_notice(raw, **kwargs):
	text = []
	extract_text(hop.models.GCNTextNotice.deserialize(raw).fields, text, **kwargs)
	return text


def extract_indexable_text_voevent(raw, **kwargs):
	text = []
	decoded = hop.models.VOEvent.deserialize(raw)
	extract_text(decoded.Who, text, **kwargs)
	extract_text(decoded.What, text, **kwargs)
	extract_text(decoded.WhereWhen, text, **kwargs)
	extract_text(decoded.How, text, **kwargs)
	extract_text(decoded.Why, text, **kwargs)
	extract_text(decoded.Citations, text, **kwargs)
	extract_text(decoded.Description, text, **kwargs)
	extract_text(decoded.Reference, text, **kwargs)
	return text


text_extractors = {
	"application/avro": extract_indexable_text_avro,
	"application/json": extract_indexable_text_json,
	"text/plain": extract_indexable_text_plain,
	"application/x.gcn.circular": extract_indexable_text_circular,
	"application/x.gcn.notice": extract_indexable_text_notice,
	"application/x.voevent": extract_indexable_text_voevent,
}
