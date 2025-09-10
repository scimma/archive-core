import bisect
import codecs
from io import BytesIO, StringIO
import struct
import zlib

class MMView:
	"""This class is a segmented memoryview, with some additional features to allow it to be used in
	more of the places where bytes can (and memoryview cannot) be. it stores a sequence of
	memoryviews, and attempts to do operations without copying the data from them whenever possible.
	"""

	def __init__(self, data=None):
		# self.data is the list of data chunks (which are views)
		# self.sizes is the list of _cumulative_ sizes
		if data is None:
			self.data = []
			self.sizes = []
		elif isinstance(data, bytes) or isinstance(data, bytearray):
			self.data = [memoryview(data)]
			self.sizes = [len(data)]
		elif isinstance(data, memoryview):
			self.data = [data]
			self.sizes = [len(data)]
		elif isinstance(data, MMView):
			self.data = [d for d in data.data]
			self.sizes = [s for s in data.sizes]
		else:
			raise ValueError()

	def append(self, data):
		if isinstance(data, bytes) or isinstance(data, bytearray):
			self.data.append(memoryview(data))
		elif isinstance(data, memoryview):
			self.data.append(data)
		elif isinstance(data, MMView):
			old_size = len(self)
			self.data.extend(data.data)
			for size in data.sizes:
				self.sizes.append(size + old_size)
			return
		else:
			raise ValueError(f"Not able to append object of type {type(data)} to an MMView")
		self.sizes.append(len(self) + len(data))
			
	def __len__(self):
		if len(self.sizes) == 0:
			return 0
		return self.sizes[-1]

	def __eq__(self, other):
		if len(self) != len(other):
			return False
		# not efficient; this is expected to be used mostly for testing
		for i in range(0, len(self)):
			if self[i] != other[i]:
				return False
		return True

	def __bytes__(self):
		if len(self.data) == 0:
			return b""
		if len(self.data) == 1:
			return bytes(self.data[0])
		buf = BytesIO()
		for chunk in self.data:
			buf.write(bytes(chunk))
		return buf.getvalue()

	def __repr__(self):
		return f"MMView of {len(self)} bytes of data in {len(self.sizes)} chunks"

	def __buffer__(self, flags):
		if len(self.data) == 1:
			return self.data[0].__buffer__(flags)
		else:
			# there appears to be no way to implement this without copying
			return memoryview(bytes(self))

	def __getitem__(self, idx):
		if isinstance(idx, slice):
			start = idx.start
			if start is None:
				start = 0
			if start >= len(self) or start < 0:
				raise IndexError()
			if idx.step is not None and idx.step != 1:
				raise IndexError("Non-unit steps are not supported")
			if idx.stop > len(self) or idx.stop < 0:
				raise IndexError()
			# find the chunk in which the start lies
			i = bisect.bisect_right(self.sizes, start)
			# compute the adjusted index within that chunk
			base = self.sizes[i - 1] if i else 0
			local_start = start - (self.sizes[i - 1] if i else 0)
			length = idx.stop - start
			if length < (self.sizes[i] - base) - local_start:
				# the slice is entirely with in this chunk
				local_stop = local_start + length
				return MMView(self.data[i][slice(local_start, local_stop, 1)])
			else:
				# ugly case: slice extends beyond this chunk
				# need to build a new object holding the data
				result = MMView()
				acc = 0  # bytes accumulated so far
				while acc < length:
					base = self.sizes[i - 1] if i else 0
					avail = (self.sizes[i] - base) - local_start
					if avail < (length - acc):
						result.append(self.data[i][local_start:])
						acc += avail
					else:
						result.append(self.data[i][local_start:((length - acc) + local_start)])
						break
					local_start = 0
					i += 1
				return result
					
		else:
			if idx >= len(self):
				raise IndexError()
			if idx < 0:
				raise IndexError()
			# find the chunk in which the index lies
			i = bisect.bisect_right(self.sizes, idx)
			# compute the adjusted index within that chunk
			local_idx = idx - (self.sizes[i - 1] if i else 0)
			return self.data[i][local_idx]

	async def generate(self, max_chunk_size=1024*1024):
		i = 0
		for datum in self.data:
			if len(datum) < max_chunk_size:
				yield datum
			else:
				offset = 0
				full_len = len(datum)
				while offset < full_len:
					length = max_chunk_size
					if offset + length > full_len:
						length = full_len - offset
					yield datum[offset:offset+length]
					offset += length
			i += 1

	def index(self, x, i=0, j=-1):
		if j == -1:
			j = len(self)
		if i < 0 or i >= len(self) or j < 0 or j > len(self):
			raise IndexError()
		if not isinstance(x, int):
			raise NotImplementedError("multi-byte targets are not supported")
		if x < 0 or x > 255:
			raise ValueError()
		for k in range(i, j):
			if self[k] == x:
				return k
		raise ValueError()

	def decode(self, *args, **kwargs):
		if len(self.data) == 1:
			return codecs.decode(self.data[0], *args, **kwargs)
		else:
			buf = StringIO()
			for datum in self.data:
				buf.write(codecs.decode(datum, *args, **kwargs))
			return buf.getvalue()

	def crc32(self):
		crc = 0
		for chunk in self.data:
			crc = zlib.crc32(chunk, crc) & 0xFFFFFFFF
		return crc


def encode_lazy_bson(obj: dict):
	"""Encode a dictionary as BSON, stored in an MMView.
	If items within the dictionary are themselves MMViews, those will be incorporated into the
	output (as binary items).
	"""
	def encode_element(buf, bson_type, name, value=None, simple_value_encoder=None):
		size = 1
		buf.write(bson_type)
		if isinstance(name, str):
			name = name.encode("utf-8")
		size += len(name) + 1
		buf.write(name)
		buf.write(b"\x00")
		if value is not None and simple_value_encoder is not None:
			size += simple_value_encoder(buf, value)
		return size

	def encode_double(buf, val):
		buf.write(struct.pack("<d", val))
		return 8
	
	def encode_string(buf, val):
		enc = val.encode("utf-8")
		s_size = len(enc) + 1
		buf.write(struct.pack("<i", s_size))
		buf.write(enc)
		buf.write(b"\x00")
		return 4 + s_size

	def encode_binary_header_only(buf, val):
		b_size = len(val)
		buf.write(struct.pack("<i", b_size))
		buf.write(b"\x00")	# always use subtype 0, "Generic binary"
		return 5

	def encode_binary(buf, val):
		h_size = encode_binary_header_only(buf, val)
		buf.write(val)
		return h_size + len(val)

	def encode_bool(buf, val):
		buf.write(struct.pack("<b", val))
		return 1

	def encode_int32(buf, val):
		buf.write(struct.pack("<i", val))
		return 4

	def encode_int64(buf, val):
		buf.write(struct.pack("<q", val))
		return 8

	def encode_uint64(buf, val):
		buf.write(struct.pack("<Q", val))
		return 8

	def encode_document(output, iter):
		# A document must start with a 32 bit size, and end with a NUL byte.
		doc_size = 5
		doc_size_buf = bytearray(4)	 # enough space to hold an int32
		output.append(doc_size_buf)
		
		rbuf = None
		def running_buffer():
			nonlocal rbuf
			if rbuf is None:
				rbuf = BytesIO()
			return rbuf
		def end_buffer():
			nonlocal rbuf
			if rbuf is not None:
				output.append(rbuf.getvalue())
			rbuf = None
		
		for name, value in iter:
			if isinstance(value, float):
				doc_size += encode_element(running_buffer(), b"\x01", name, value, encode_double)
			elif isinstance(value, str):
				doc_size += encode_element(running_buffer(), b"\x02", name, value, encode_string)
			elif isinstance(value, dict):
				# trying to thread the running buffer through a recursive call is too much trouble
				# for now
				doc_size += encode_element(running_buffer(), b"\x03", name)
				end_buffer()
				doc_size += encode_document(output, value.items())
			elif isinstance(value, list) or isinstance(value, tuple):
				# trying to thread the running buffer through a recursive call is too much trouble
				# for now
				doc_size += encode_element(running_buffer(), b"\x04", name)
				end_buffer()
				doc_size += encode_document(output, zip((str(i) for i in range(len(value))), value))
			elif isinstance(value, bytes):
				if len(value) < 1024:
					doc_size += encode_element(running_buffer(), b"\x05", name, value, encode_binary)
				else:
					doc_size += encode_element(running_buffer(), b"\x05", name, value, encode_binary_header_only)
					end_buffer()
					doc_size += len(value)
					output.append(value)
			elif isinstance(value, memoryview) or isinstance(value, MMView):
				doc_size += encode_element(running_buffer(), b"\x05", name, value, encode_binary_header_only)
				end_buffer()
				doc_size += len(value)
				output.append(value)
			# Won't implement: Undefined
			# Won't implement: ObjectID
			elif isinstance(value, bool):
				doc_size += encode_element(running_buffer(), b"\x08", name, value, encode_bool)
			# TODO: datetime
			elif value is None:
				doc_size += encode_element(running_buffer(), b"\x0A", name)
			# Regex would go here, but probably no one cares?
			# Won't implement: DB Pointer
			# Won't implement: Javascript
			# Won't implement: Symbol
			# Won't implement: Javascript with scope
			elif isinstance(value, int):
				if value<=0x7FFFFFFF and value>=-0x80000000:
					doc_size += encode_element(running_buffer(), b"\x10", name, value, encode_int32)
				elif value<=0x7FFFFFFFFFFFFFFF and value>=-0x8000000000000000:
					doc_size += encode_element(running_buffer(), b"\x12", name, value, encode_int64)
				elif value>=0 and value<=0xFFFFFFFFFFFFFFFF:
					doc_size += encode_element(running_buffer(), b"\x11", name, value, encode_uint64)
				else:
					raise ValueError("Integer outside of BSON-encodable range")
			# decimal 128 would go here, but probably no one cares
			# min key would go here, but probably no one cares?
			# max key would go here, but probably no one cares?
			else:
				raise ValueError(f"Not able to encode {value} as BSON")

		# NUL terminator for document
		if rbuf is not None:
			rbuf.write(b"\x00")
			end_buffer()
		else:
			output.append(b"\x00")
		# overwrite the size buffer with the actual size data
		doc_size_buf[0:4] = struct.pack("<i", doc_size)
		return doc_size
	
	result = MMView()
	result.total_size = encode_document(result, obj.items())
	return result
