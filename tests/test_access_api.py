import argparse
import bson
from hop.io import Metadata
import pytest
import uuid

from archive import access_api
from archive import store_api

from conftest import temp_environ

pytest_plugins = ('pytest_asyncio',)

def test_add_parser_options(tmpdir):
    parser = argparse.ArgumentParser()
    access_api.add_parser_options(parser)
    config = parser.parse_args([])
    assert not config.read_only, "Read-only should not be enabled by default"
    
    config = parser.parse_args(["--read-only", "true"])
    assert config.read_only, "Read-only should be enabled by the correct option"
    
    with temp_environ(READ_ONLY="true"):
        parser = argparse.ArgumentParser()
        access_api.add_parser_options(parser)
        config = parser.parse_args([])
        assert config.read_only, "Read-only should be enabled by the correct environment variable"

def get_mock_config():
    return {"db_type": "mock",
            "store_type": "mock",
            "store_primary_bucket": "archive",
            "store_backup_bucket": "backup",
	        "store_region_name": "eu-north-3"}

@pytest.mark.asyncio
async def test_archive_access_startup_shutdown():
    aa = access_api.Archive_access(get_mock_config())
    assert not aa.read_only
    assert not aa.db.connected
    assert not aa.store.connected
    await aa.connect()
    assert aa.db.connected
    assert not aa.db.read_only
    assert aa.store.connected
    assert not aa.store.read_only
    
    await aa.close()
    assert not aa.db.connected
    assert not aa.store.connected
    
    ro_conf = get_mock_config()
    ro_conf["read_only"] = True
    aa = access_api.Archive_access(ro_conf)
    assert aa.read_only
    await aa.connect()
    assert aa.db.read_only
    assert aa.store.read_only

@pytest.mark.asyncio
async def test_archive_access_store_message():
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
    message = b"datadatadata"
    metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
    r = await aa.store_message(message, metadata)
    assert r[0], "Insertion should succeed"
    
    dr = await aa.db.fetch(str(u))
    assert dr is not None, "Message should be recorded in database"
    assert dr.topic == metadata.topic
    assert dr.timestamp == metadata.timestamp
    
    sr = await aa.store.get_object(dr.key)
    assert sr is not None, "Message should be in data store"
    data = bson.loads(sr)
    assert data["message"] == message
    assert data["annotations"]["con_text_uuid"] == str(u)

@pytest.mark.asyncio
async def test_archive_access_store_message_no_uuid():
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    message = b"datadatadata"
    metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[], _raw=None)
    r = await aa.store_message(message, metadata)
    assert r[0], "Insertion should succeed"
    
    dr, _, _ = await aa.get_message_records(topics_full=["t1"])
    assert len(dr) == 1, "Message should be recorded in database"
    assert dr[0].topic == metadata.topic
    assert dr[0].timestamp == metadata.timestamp
    
    sr = await aa.store.get_object(dr[0].key)
    assert sr is not None, "Message should be in data store"
    data = bson.loads(sr)
    assert data["message"] == message

@pytest.mark.asyncio
async def test_archive_access_store_message_duplicate_uuid():
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
    message = b"datadatadata"
    metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
    r = await aa.store_message(message, metadata)
    assert r[0], "Insertion should succeed"
    
    r = await aa.store_message(message, metadata)
    assert not r[0], "Repeated insertion should fail"

@pytest.mark.asyncio
async def test_archive_access_store_message_duplicate_wo_uuid():
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    message = b"datadatadata"
    metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[], _raw=None)
    r = await aa.store_message(message, metadata)
    assert r[0], "Insertion should succeed"
    
    r = await aa.store_message(message, metadata)
    assert not r[0], "Repeated insertion should fail"

@pytest.mark.asyncio
async def test_archive_access_get_metadata():
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
    message = b"datadatadata"
    metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
    r = await aa.store_message(message, metadata)
    assert r[0], "Insertion should succeed"
    
    r = await aa.get_metadata(str(u))
    assert r is not None, "Message should be found"
    assert r.topic == metadata.topic
    assert r.timestamp == metadata.timestamp
    assert r.uuid == str(u)

@pytest.mark.asyncio
async def test_archive_access_get_topics_with_public_messages():
    messages = []
    for i in range(0,10):
        ms = b"datadatadata"
        md = Metadata(topic=f"t{i}", partition=0, offset=i, timestamp=i, key="", headers=[("_id",uuid.uuid4().bytes)], _raw=None)
        messages.append((ms,md))
    
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    for m in messages:
        await aa.store_message(m[0],m[1], public=(m[1].offset % 2 == 0))
    
    pub_top = await aa.get_topics_with_public_messages()
    assert len(pub_top) == 5

@pytest.mark.asyncio
async def test_archive_access_get_message_records():
    messages = []
    for i in range(0,10):
        ms = b"datadatadata"
        md = Metadata(topic="t1", partition=0, offset=i, timestamp=i, key="", headers=[("_id",uuid.uuid4().bytes)], _raw=None)
        messages.append((ms,md))
    
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    for m in messages:
        await aa.store_message(m[0],m[1])
    
    r, _, _ = await aa.get_message_records(topics_full=["t1"], start_time=4, end_time=7)
    assert len(r) == 3
    assert r[0].timestamp == 4
    assert r[1].timestamp == 5
    assert r[2].timestamp == 6
    
    r, n, p = await aa.get_message_records(topics_full=["t1"], start_time=3, end_time=7, page_size=2)
    assert len(r) == 2
    assert r[0].timestamp == 3
    assert r[1].timestamp == 4
    assert n is not None
    assert p is None
    
    r, n, p = await aa.get_message_records(topics_full=["t1"], start_time=3, end_time=7, page_size=2, bookmark=n)
    assert len(r) == 2
    assert r[0].timestamp == 5
    assert r[1].timestamp == 6
    assert n is None
    assert p is not None
    
    r, n, p = await aa.get_message_records(topics_full=["t1"], start_time=12, end_time=14)
    assert len(r) == 0
    assert n is None
    assert p is None
    
    r, n, p = await aa.get_message_records(topics_full=["t2"], start_time=0, end_time=5)
    assert len(r) == 0
    assert n is None
    assert p is None

@pytest.mark.asyncio
async def test_archive_access_count_message_records():
    messages = []
    for i in range(0,10):
        ms = b"datadatadata"
        md = Metadata(topic="t1", partition=0, offset=i, timestamp=i, key="", headers=[("_id",uuid.uuid4().bytes)], _raw=None)
        messages.append((ms,md))
    
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    for m in messages:
        await aa.store_message(m[0],m[1])
    
    c = await aa.count_message_records(topics_full=["t1"], start_time=4, end_time=7)
    assert c == 3
    
    c = await aa.count_message_records(topics_full=["t1"], start_time=12, end_time=14)
    assert c == 0
    
    c = await aa.count_message_records(topics_full=["t2"], start_time=0, end_time=5)
    assert c == 0

@pytest.mark.asyncio
async def test_archive_access_get_object_lazily():
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
    message = b"datadatadata"
    metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
    r = await aa.store_message(message, metadata)
    
    key = (await aa.get_metadata(str(u))).key
    r = await aa.get_object_lazily(key)
    assert r is not None, "Message should be found"
    assert isinstance(r, store_api.Mock_store.LazyObject)
    obj = r.read()
    data = bson.loads(obj)
    assert data["message"] == message
    
    not_found = await aa.get_object_lazily("some invalid key")
    assert not_found is None

@pytest.mark.asyncio
async def test_archive_access_get_raw_object():
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
    message = b"datadatadata"
    metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
    r = await aa.store_message(message, metadata)
    
    key = (await aa.get_metadata(str(u))).key
    obj = await aa.get_raw_object(key)
    assert obj is not None, "Message should be found"
    data = bson.loads(obj)
    assert data["message"] == message
    
    not_found = await aa.get_raw_object("some invalid key")
    assert not_found is None

@pytest.mark.asyncio
async def test_archive_access_get_all():
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
    message = b"datadatadata"
    metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
    r = await aa.store_message(message, metadata)
    assert r[0], "Insertion should succeed"
    
    key = (await aa.get_metadata(str(u))).key
    r = await aa.get_all(key)
    assert r is not None, "Message should be found"
    message_out, metadata_out = r
    assert message_out == message
    assert metadata_out["topic"] == metadata.topic
    assert metadata_out["timestamp"] == metadata.timestamp
    
    not_found = await aa.get_all("some invalid key")
    assert not_found is None

@pytest.mark.asyncio
async def test_archive_access_get_as_sent():
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
    message = b"datadatadata"
    metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
    r = await aa.store_message(message, metadata)
    
    key = (await aa.get_metadata(str(u))).key
    r = await aa.get_as_sent(key)
    assert r is not None, "Message should be found"
    message_out, headers_out = r
    assert message_out == message
    assert len(headers_out) == len(metadata.headers)
    assert len(headers_out[0]) == len(metadata.headers[0])
    assert headers_out[0][0] == metadata.headers[0][0]
    assert headers_out[0][1] == metadata.headers[0][1]
    
    not_found = await aa.get_as_sent("some invalid key")
    assert not_found is None

@pytest.mark.asyncio
async def test_archive_access_get_object_summary():
    aa = access_api.Archive_access(get_mock_config())
    await aa.connect()
    
    u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
    message = b"datadatadata"
    metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
    r = await aa.store_message(message, metadata)
    
    key = (await aa.get_metadata(str(u))).key
    r = await aa.get_object_summary(key)
    assert r is not None, "Message should be found"
    assert r["exists"]
    assert r["size"] > 0
    
    not_found = await aa.get_object_summary("some invalid key")
    assert not not_found["exists"]