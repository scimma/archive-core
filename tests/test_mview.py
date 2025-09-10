import bson
import pytest
from zlib import crc32

from archive.mview import MMView, encode_lazy_bson

pytest_plugins = ('pytest_asyncio',)

def test_mmview_construct_empty():
	m = MMView()
	assert len(m.data) == 0
	assert len(m.sizes) == 0

def test_mmview_construct_bytes():
	d = b"0123"
	m = MMView(d)
	assert len(m.data) == 1
	assert len(m.sizes) == 1
	assert isinstance(m.data[0], memoryview)
	assert m.data[0].obj is d

def test_mmview_construct_bytearray():
	d = bytearray(b"0123")
	m = MMView(d)
	assert len(m.data) == 1
	assert len(m.sizes) == 1
	assert isinstance(m.data[0], memoryview)
	assert m.data[0].obj is d

def test_mmview_construct_memoryview():
	d = b"0123"
	v = memoryview(d)
	m = MMView(v)
	assert len(m.data) == 1
	assert len(m.sizes) == 1
	assert isinstance(m.data[0], memoryview)
	assert m.data[0].obj is d

def test_mmview_construct_mmview():
	m1 = MMView()
	m1.append(b"A")
	m1.append(b"BB")
	m1.append(b"CCC")
	m1.append(b"DDDD")
	m2 = MMView(m1)
	assert len(m2.data) == 4
	assert len(m2.sizes) == 4
	for entry in m2.data:
		assert isinstance(entry, memoryview)
	assert m2.sizes == [1, 3, 6, 10]
	assert m2 == b"ABBCCCDDDD"

def test_mmview_construct_invalid():
	for obj in ["abc", [1, 2, 3], {"foo": "bar"}]:
		with pytest.raises(ValueError):
			m = MMView(obj)

def test_mmview_append_bytes():
	m = MMView()
	m.append(b"AAA")
	assert len(m.data) == 1
	assert len(m.sizes) == 1
	assert m.sizes[0] == 3
	m.append(b"BBBB")
	assert len(m.data) == 2
	assert len(m.sizes) == 2
	assert m.sizes[0] == 3
	assert m.sizes[1] == 7
	assert m == b"AAABBBB"

def test_mmview_append_bytearray():
	m = MMView()
	m.append(b"AAA")
	assert len(m.data) == 1
	assert len(m.sizes) == 1
	assert m.sizes[0] == 3
	m.append(bytearray(b"BBBB"))
	assert len(m.data) == 2
	assert len(m.sizes) == 2
	assert m.sizes[0] == 3
	assert m.sizes[1] == 7
	assert m == b"AAABBBB"

def test_mmview_append_memoryview():
	m = MMView()
	m.append(b"AAA")
	assert len(m.data) == 1
	assert len(m.sizes) == 1
	assert m.sizes[0] == 3
	m.append(memoryview(b"BBBB"))
	assert len(m.data) == 2
	assert len(m.sizes) == 2
	assert m.sizes[0] == 3
	assert m.sizes[1] == 7
	assert m == b"AAABBBB"

def test_mmview_append_mmview():
	m1 = MMView(b"A")
	m1.append(b"BB")
	m2 = MMView(b"CCC")
	m2.append(b"DDDD")
	
	m = MMView()
	m.append(m1)
	assert len(m.data) == 2
	assert len(m.sizes) == 2
	assert m.sizes[0] == 1
	assert m.sizes[1] == 3
	assert m == b"ABB"
	
	m.append(m2)
	assert len(m.data) == 4
	assert len(m.sizes) == 4
	assert m.sizes[0] == 1
	assert m.sizes[1] == 3
	assert m.sizes[2] == 6
	assert m.sizes[3] == 10
	assert m == b"ABBCCCDDDD"

def test_mmview_append_invalid():
	m = MMView()
	for obj in ["abc", [1, 2, 3], {"foo": "bar"}]:
		with pytest.raises(ValueError):
			m.append(obj)

def test_mmview_len():
	m = MMView()
	assert len(m) == 0
	m.append(b"A")
	assert len(m) == 1
	m.append(b"BB")
	assert len(m) == 3
	m.append(b"CCC")
	assert len(m) == 6
	m.append(b"DDDD")
	assert len(m) == 10

def test_mmview_eq():
	m = MMView()
	assert m == b""
	assert m != b"A"
	
	m.append(b"A")
	assert m == b"A"
	assert m != b""
	assert m != b"B"
	assert m != b"AA"
	
	m.append(b"BB")
	assert m == b"ABB"
	assert m != b"AB"
	assert m != b"AAB"
	assert m != b"ABBB"
	
	m.append(b"CCC")
	assert m == b"ABBCCC"
	assert m != b"ABBCC"
	assert m != b"ABBCCD"
	assert m != b"ABBCCCC"

def test_mmview_bytes():
	m = MMView()
	assert bytes(m) == b""
	
	m.append(b"A")
	assert bytes(m) == b"A"
	
	m.append(bytearray(b"BB"))
	assert bytes(m) == b"ABB"
	
	m.append(memoryview(b"XCCCX")[1:4])
	assert bytes(m) == b"ABBCCC"

def test_mmview_repr():
	m = MMView(b"A")
	m.append(b"BB")
	m.append(b"CCC")
	m.append(b"DDDD")
	desc = repr(m)
	assert " 10 bytes of data" in desc
	assert "in 4 chunks" in desc

def test_mmview_buffer():
	m = MMView()
	v = memoryview(m)
	assert len(v) == 0
	
	m.append(b"AAA")
	v = memoryview(m)
	assert len(v) == 3
	
	m.append(b"BBBB")
	v = memoryview(m)
	assert len(v) == 7

def test_mmview_getitem_single():
	m = MMView(b"\x00")
	m.append(b"\x01\x02")
	m.append(b"\x03\x04\x05")
	m.append(b"\x06\x07\x08\x09")
	for i in range(0, 10):
		assert m[i] == i

def test_mmview_getitem_single_invalid():
	m = MMView(b"\x00")
	m.append(b"\x01\x02")
	m.append(b"\x03\x04\x05")
	m.append(b"\x06\x07\x08\x09")
	for bad_idx in [-2, -1, 10, 11]:
		with pytest.raises(IndexError):
			m[bad_idx]

def test_mmview_getitem_slice():
	m = MMView(b"\x00")
	m.append(b"\x01\x02")
	m.append(b"\x03\x04\x05")
	m.append(b"\x06\x07\x08\x09")
	for start in range(0,10):
		for stop in range(start + 1, 11):
			print(f" Extracting slice({start},{stop})")
			s = m[start:stop]
			assert len(s) == (stop - start)
			for i in range(0, stop - start):
				print(f"  Checking slice element {i}")
				assert s[i] == (start + i)

def test_mmview_getitem_slice_no_start():
	m = MMView(b"\x00")
	m.append(b"\x01\x02")
	m.append(b"\x03\x04\x05")
	m.append(b"\x06\x07\x08\x09")
	for stop in range(1,10):
		s = m[:stop]
		assert len(s) == stop
		for i in range(0, stop):
				assert s[i] == i

def test_mmview_getitem_slice_invalid():
	m = MMView(b"ABBCCCDDDD")
	with pytest.raises(IndexError):
		m[-1:5]
	with pytest.raises(IndexError):
		m[10:5]
	with pytest.raises(IndexError):
		m[1:2:7]
	with pytest.raises(IndexError):
		m[2:-2]
	with pytest.raises(IndexError):
		m[2:12]

@pytest.mark.asyncio
async def test_mmview_generate():
	m = MMView(b"A")
	m.append(b"BB")
	m.append(b"CCC")
	m.append(b"DDDD")
	
	chunks = []
	async for chunk in m.generate():
		chunks.append(chunk)
	
	assert len(chunks) == 4
	assert len(chunks[0]) == 1
	assert len(chunks[1]) == 2
	assert len(chunks[2]) == 3
	assert len(chunks[3]) == 4
	assert bytes(chunks[0]) == b"A"
	assert bytes(chunks[1]) == b"BB"
	assert bytes(chunks[2]) == b"CCC"
	assert bytes(chunks[3]) == b"DDDD"

@pytest.mark.asyncio
async def test_mmview_generate_max_size():
	m = MMView(b"A")
	m.append(b"BB")
	m.append(b"CCC")
	m.append(b"DDDD")
	
	chunks = []
	async for chunk in m.generate(max_chunk_size=2):
		chunks.append(chunk)
	
	assert len(chunks) == 6
	assert len(chunks[0]) == 1
	assert len(chunks[1]) == 2
	assert len(chunks[2]) == 2
	assert len(chunks[3]) == 1
	assert len(chunks[4]) == 2
	assert len(chunks[5]) == 2
	assert bytes(chunks[0]) == b"A"
	assert bytes(chunks[1]) == b"BB"
	assert bytes(chunks[2]) == b"CC"
	assert bytes(chunks[3]) == b"C"
	assert bytes(chunks[4]) == b"DD"
	assert bytes(chunks[5]) == b"DD"

def test_mmview_index():
	m = MMView(b"A")
	m.append(b"BB")
	m.append(b"CCC")
	m.append(b"DEED")
	
	assert m.index(65) == 0
	assert m.index(66) == 1
	assert m.index(67) == 3
	assert m.index(68) == 6
	assert m.index(69) == 7
	with pytest.raises(ValueError):
		m.index(70)
	
	with pytest.raises(ValueError):
		m.index(65, 1)
	assert m.index(66, 2) == 2
	assert m.index(67, 4) == 4
	assert m.index(68, 7) == 9
	
	with pytest.raises(ValueError):
		m.index(67, 0, 3)

def test_mmview_index_invalid():
	m = MMView(b"A")
	m.append(b"BB")
	m.append(b"CCC")
	m.append(b"DEED")
	
	with pytest.raises(IndexError):
		m.index(65, -1)
	with pytest.raises(IndexError):
		m.index(65, 10)
	with pytest.raises(IndexError):
		m.index(65, 0, -2)
	with pytest.raises(IndexError):
		m.index(65, 0, 11)

	with pytest.raises(ValueError):
		m.index(-7)
	with pytest.raises(ValueError):
		m.index(289)

	with pytest.raises(NotImplementedError):
		m.index(b"BC")

def test_mmview_decode():
	assert MMView(b"ABCD").decode("utf-8") == "ABCD"
	
	m = MMView(b"A")
	m.append(b"BB")
	m.append(b"CCC")
	m.append(b"DDDD")
	assert m.decode("utf-8") == "ABBCCCDDDD"

def test_mmview_crc32():
	m = MMView(b"A")
	m.append(b"BB")
	m.append(b"CCC")
	m.append(b"DDDD")
	assert m.crc32() == crc32(b"ABBCCCDDDD")

def check_bson(obj):
	lazy = encode_lazy_bson(obj)
	eager = bson.dumps(obj)
	assert lazy == eager

def test_encode_lazy_bson_empty():
	check_bson({})

def test_encode_lazy_bson_float():
	check_bson({"foo": 3.1415})

def test_encode_lazy_bson_str():
	check_bson({"foo": "bar"})

def test_encode_lazy_bson_dict():
	check_bson({"foo": {"bar": 1.7, "baz": 4.8}})

def test_encode_lazy_bson_list():
	check_bson({"foo": ["bar", 1.5, "baz", -6.0]})

def test_encode_lazy_bson_tuple():
	check_bson({"foo": ("bar", 1.5, "baz", -6.0)})

def test_encode_lazy_bson_short_bytes():
	check_bson({"foo": b"ABCD"})

def test_encode_lazy_bson_long_bytes():
	check_bson({"foo": b"ABCD"*512})

def obj_index(container, target):
	index = 0
	for item in container:
		if item is target:
			return index
		index += 1
	raise ValueError(f"{target} not found in {container}")

def test_encode_lazy_bson_memoryview():
	# bson.dumps cannot handle memoryviews, so we must simulate what it should do
	d = b"ABCD"
	m = memoryview(d)
	lazy = encode_lazy_bson({"foo": m})
	eager = bson.dumps({"foo": d})
	assert lazy == eager
	# the lazy data stream should include the original memoryview, this is the whole point of it
	assert obj_index(lazy.data, m) is not None

def test_encode_lazy_bson_MMView():
	# bson.dumps cannot handle memoryviews, so we must simulate what it should do
	m = MMView(b"A")
	m.append(b"BB")
	m.append(b"CCC")
	m.append(b"DDDD")
	lazy = encode_lazy_bson({"foo": m})
	eager = bson.dumps({"foo": bytes(m)})
	assert lazy == eager
	# the lazy data stream should include the original memoryviews, this is the whole point of it
	i = 0
	idx = None
	for chunk in m.data:
		if idx is None:
			idx = obj_index(lazy.data, chunk)
		else:
			assert obj_index(lazy.data, chunk) == idx
		idx = idx + 1
		i += 1

def test_encode_lazy_bson_bool():
	check_bson({"true": True, "false": False})

def test_encode_lazy_bson_None():
	check_bson({"foo": None})

def test_encode_lazy_bson_int32():
	check_bson({"foo": 0})
	check_bson({"foo": 1})
	check_bson({"foo": -1})
	check_bson({"foo": 65536})
	check_bson({"foo": -65536})
	check_bson({"foo": 2147483647})
	check_bson({"foo": -2147483648})

def test_encode_lazy_bson_int64():
	check_bson({"foo": 2147483648})
	check_bson({"foo": -2147483649})
	check_bson({"foo": 9223372036854775807})
	check_bson({"foo": -9223372036854775808})

def test_encode_lazy_bson_uint64():
	check_bson({"foo": 9223372036854775808})
	check_bson({"foo": 18446744073709551615})

def test_encode_lazy_bson_int_out_of_range():
	with pytest.raises(ValueError):
		encode_lazy_bson({"foo": 18446744073709551616})
	with pytest.raises(ValueError):
		encode_lazy_bson({"foo": -9223372036854775809})

def test_encode_lazy_bson_invalid_type():
	with pytest.raises(ValueError):
		encode_lazy_bson({"foo": bson})  # can't encode a module