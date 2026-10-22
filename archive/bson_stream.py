"""
Low-level streaming BSON parsing helper.

Message records are stored as a single BSON-encoded bundle containing the
message body, its headers, and (if present) its file attachment together.
extract_binary_field() streams through such a bundle, chunk by chunk,
without ever buffering the whole document in memory, and yields only the
bytes of one named top-level binary field - by default "message", which is
where a file attachment's raw bytes live when one was sent.
"""
import enum
import logging
import struct


@enum.unique
class BSONType(enum.Enum):
    double =      0x01
    string =      0x02
    document =    0x03
    array =       0x04
    binary =      0x05
    undefined =   0x06
    object_id =   0x07
    bool =        0x08
    datetime =    0x09
    null =        0x0A
    regex =       0x0B
    db_pointer =  0x0C
    javascript =  0x0D
    symbol =      0x0E
    scoped_code = 0x0F
    int32 =       0x10
    timestamp =   0x11
    int64 =       0x12
    decimal128 =  0x13
    min_key =     0xFF
    max_key =     0x7F


async def extract_binary_field(s3_result, field_name: str = "message"):
    """
    Stream through a BSON document (as returned by an S3-style lazy object
    fetch, i.e. a dict with an async-iterable 'Body') and yield the raw
    bytes of the named top-level binary field, without buffering the whole
    document in memory.

    s3_result: dict with an async-iterable 'Body' of byte chunks, as
               returned by store_api's get_object_lazily().
    field_name: the top-level BSON field to extract (default "message",
                where a file attachment's bytes are stored).
    """
    def anext(iterator, default=None):
        __anext__ = type(iterator).__anext__
        async def anext_impl():
            try:
                return await __anext__(iterator)
            except StopAsyncIteration:
                return default
        return anext_impl()

    iterator = s3_result['Body'].__aiter__()
    current_chunk = await anext(iterator, None)
    if current_chunk is None:
        return

    current_offset = 0
    len_remaining = 0

    async def read(amount: int):
        nonlocal current_chunk
        nonlocal current_offset
        nonlocal len_remaining
        value = bytearray()
        while current_chunk is not None and amount > 0:
            clen = len(current_chunk)
            if amount <= clen - current_offset:
                value += current_chunk[current_offset:current_offset+amount]
                current_offset += amount
                amount = 0
            else:
                value += current_chunk[current_offset:]
                amount -= (clen - current_offset)
                current_offset = clen
            if current_offset >= clen:
                current_chunk = await anext(iterator, None)
                current_offset = 0
        if current_chunk is None and amount > 0:
            raise ValueError("read: Early end of data stream")
        len_remaining -= len(value)
        return value

    async def ignore(amount: int):
        nonlocal current_chunk
        nonlocal current_offset
        nonlocal len_remaining
        while current_chunk is not None and amount > 0:
            clen = len(current_chunk)
            if amount <= clen - current_offset:
                current_offset += amount
                len_remaining -= amount
                amount = 0
            else:
                amount -= (clen - current_offset)
                len_remaining -= (clen - current_offset)
                current_offset = clen
            if current_offset >= clen:
                current_chunk = await anext(iterator, None)
                current_offset = 0
        if current_chunk is None and amount > 0:
            raise ValueError("ignore: Early end of data stream")

    async def send(amount: int):
        nonlocal current_chunk
        nonlocal current_offset
        nonlocal len_remaining
        while current_chunk is not None and amount > 0:
            clen = len(current_chunk)
            if amount <= clen - current_offset:
                yield current_chunk[current_offset:current_offset+amount]
                current_offset += amount
                len_remaining -= amount
                amount = 0
            else:
                yield current_chunk[current_offset:]
                amount -= (clen - current_offset)
                len_remaining -= (clen - current_offset)
                current_offset = clen
            if current_offset >= clen:
                current_chunk = await anext(iterator, None)
                current_offset = 0
        if current_chunk is None and amount > 0:
            raise ValueError("send: Early end of data stream")

    async def read_cstring():
        value = bytearray()
        while True:
            c = await read(1)
            if c == b'\x00':
                return value.decode("utf-8")
            value.extend(c)

    def decode_int32(data):
        return struct.unpack("<i", data)[0]

    try:
        len_remaining = decode_int32(await read(4))
        len_remaining -= 4
        while len_remaining > 1:  # expect one byte for end of document
            raw_e_type = await read(1)
            if raw_e_type[0] == 0:  # end of document
                break
            if len_remaining < 2:
                raise ValueError("Malformed document")
            try:
                e_type = BSONType(raw_e_type[0])
            except ValueError:
                logging.warning(f"Unexpected element type for BSON 1.1: 0x{raw_e_type.hex()}")
                return
            e_name = await read_cstring()
            if len_remaining < 1:
                raise ValueError("Malformed document")

            # If this is our blessed sub-object, extract it
            if e_type == BSONType.binary and e_name == field_name:
                raw_dlen = await read(4)
                dlen = decode_int32(raw_dlen)
                # plus one byte for subtype
                if dlen + 1 >= len_remaining:
                    raise ValueError("Malformed binary: embedded binary longer than remainder of document")
                # No one really cares what the subtype is; just extract and discard
                subtype = await read(1)
                async for chunk in send(dlen):
                    yield bytes(chunk)
                # We don't really care what else was in the document; we won't send it, so skip
                # reading and parsing it
                return

            # Otherwise, this is just something to skip over
            if e_type == BSONType.double or e_type == BSONType.datetime \
                    or e_type == BSONType.timestamp or e_type == BSONType.int64:
                await ignore(8)
            elif e_type == BSONType.string or e_type == BSONType.javascript \
                     or e_type == BSONType.symbol:
                slen = decode_int32(await read(4))
                if slen >= len_remaining:
                    raise ValueError("Malformed document: string longer than remainder of document")
                await ignore(slen)
            elif e_type == BSONType.document or e_type == BSONType.array:
                dlen = decode_int32(await read(4))
                if dlen < 5:
                    raise ValueError("Malformed document: Too small length for embedded document")
                dlen -= 4  # we already consumed the length
                if dlen >= len_remaining:
                    raise ValueError("Malformed document: embedded document longer than remainder of document")
                await ignore(dlen)
            elif e_type == BSONType.binary:
                raw_dlen = await read(4)
                dlen = decode_int32(raw_dlen)
                # plus one byte for subtype
                if dlen + 1 >= len_remaining:
                    raise ValueError("Malformed binary: embedded binary longer than remainder of document")
                await ignore(dlen + 1)
            elif e_type == BSONType.undefined or e_type == BSONType.null \
                    or e_type == BSONType.min_key or e_type == BSONType.max_key:
                pass  # zero size element; nothing to do
            elif e_type == BSONType.object_id:
                await ignore(12)
            elif e_type == BSONType.bool:
                await ignore(1)
            elif e_type == BSONType.regex:
                _ = await read_cstring()
                _ = await read_cstring()
            elif e_type == BSONType.db_pointer:
                slen = decode_int32(await read(4))
                if slen >= len_remaining:
                    raise ValueError("Malformed document: DB pointer string longer than remainder of document")
                await ignore(slen + 12)
            elif e_type == BSONType.scoped_code:
                clen = decode_int32(await read(4))
                if clen < 10:
                    raise ValueError("Malformed document: Too small length for code with scope")
                clen -= 4  # we already consumed the length
                if clen >= len_remaining:
                    raise ValueError("Malformed document: code with scope longer than remainder of document")
                await ignore(clen)
            elif e_type == BSONType.int32:
                await ignore(4)
            elif e_type == BSONType.decimal128:
                await ignore(16)
            else:
                raise ValueError(f"Internal Error: uncovered e_type: {e_type.value}")
    except ValueError as err:
        logging.warning(f"BSON decoding error: {err}")
        return
    except struct.error as err:
        logging.warning(f"BSON decoding error: {err}")
        return
