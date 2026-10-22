import bson
import pytest

from archive import bson_stream

pytest_plugins = ('pytest_asyncio',)


class ChunkedBody:
    """Mimics the 'Body' of an S3 response: an async iterable of byte chunks."""
    def __init__(self, data: bytes, chunk_size: int):
        self.chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    def __aiter__(self):
        async def gen():
            for chunk in self.chunks:
                yield chunk
        return gen()


async def collect(data: bytes, chunk_size: int, **kwargs) -> bytes:
    out = b""
    async for chunk in bson_stream.extract_binary_field({"Body": ChunkedBody(data, chunk_size)}, **kwargs):
        out += chunk
    return out


def make_bundle(payload: bytes) -> bytes:
    # Same shape the archive stores: the message bytes plus its headers and metadata.
    return bson.dumps({
        "headers": {"_id": b"\x01" * 16, "file_name": b"a.txt", "title": b"t", "n": 5, "f": 1.5,
                    "nested": {"x": [1, 2, 3], "s": "text"}, "flag": True, "nothing": None},
        "message": payload,
        "metadata": {"topic": "t1", "timestamp": 1792638756518},
    })


# 1 byte chunks stress every boundary; 7 is awkward; large is the single-chunk case
@pytest.mark.asyncio
@pytest.mark.parametrize("chunk_size", [1, 7, 64, 1 << 20])
async def test_extracts_message_field_for_any_chunking(chunk_size):
    payload = bytes(range(256)) * 20
    assert await collect(make_bundle(payload), chunk_size) == payload


@pytest.mark.asyncio
async def test_extracts_other_named_binary_field():
    data = bson.dumps({"message": b"AAA", "other": b"BBB"})
    assert await collect(data, 5, field_name="other") == b"BBB"


@pytest.mark.asyncio
async def test_empty_payload():
    assert await collect(make_bundle(b""), 3) == b""


@pytest.mark.asyncio
async def test_missing_field_yields_nothing():
    assert await collect(bson.dumps({"headers": {"a": 1}}), 4) == b""


@pytest.mark.asyncio
async def test_empty_body_yields_nothing():
    assert await collect(b"", 4) == b""


@pytest.mark.asyncio
async def test_truncated_document_does_not_raise():
    data = make_bundle(b"x" * 100)
    # A damaged object is logged and yields no (or partial) data, never an exception
    await collect(data[:40], 8)


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [1, 1000, 1005, 1006, 1007, 1010, 1020, 1023, 1024, 1025, 2048, 5000])
async def test_payload_sizes_around_the_chunk_size(size):
    # Regression test: a payload that fits in one chunk, but not in what is left of the
    # first chunk after the leading BSON headers, used to be silently truncated.
    # This is the shape of a real stored message (the "message" field comes first).
    payload = bytes(i % 251 for i in range(size))
    bundle = bson.dumps({"message": payload,
                         "metadata": {"topic": "t1", "timestamp": 1792638756518, "headers": {"a": b"b"}},
                         "annotations": {"bucket": "b", "crc32": 12345}})
    assert await collect(bundle, 1024) == payload
