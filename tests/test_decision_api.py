from hop.io import Metadata
from io import BytesIO
import pytest
import uuid

from archive import database_api
from archive import decision_api
from archive import store_api

pytest_plugins = ('pytest_asyncio',)

@pytest.mark.asyncio
async def test_is_content_identical():
    config = {
        "db_type": "mock",
        "store_type": "mock",
        "store_primary_bucket": "b1",
        "store_backup_bucket": "b2",
    }
    db = database_api.DbFactory(config)
    st = store_api.StoreFactory(config)
    await db.connect()
    await st.connect()
    
    m1 = (b"abcdefgh",
          Metadata(topic="t1", partition=0, offset=0, timestamp=172, key="", headers=[], _raw=None))
    m2 = (b"abcdefgh",
          Metadata(topic="t1", partition=0, offset=1, timestamp=223, key="", headers=[], _raw=None))
    m3 = (b"somethingelse",
          Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[], _raw=None))
    
    async def store(payload, metadata):
        annotations = decision_api.get_annotations(payload, metadata.headers)
        await st.store(payload, metadata, annotations)
        await db.insert(metadata, annotations)
        return annotations["con_text_uuid"]
    
    is_content_identical = decision_api.is_content_identical
    ids = []
    ids.append(await db.get_message_id(await store(*m1)))
    assert await is_content_identical(ids, db, st), "A single message is identical to itself"
    ids.append(await db.get_message_id(await store(*m2)))
    assert await is_content_identical(ids, db, st), "Messages with the same content are identical"
    ids.append(await db.get_message_id(await store(*m3)))
    assert not await is_content_identical(ids, db, st), "Messages with the different content are not identical"

def test_get_text_uuid():
    get_text_uuid = decision_api.get_text_uuid
    
    ui1 = uuid.uuid4()
    uo1 = get_text_uuid([("_id", ui1.bytes)])
    assert uo1[0] == str(ui1)
    assert uo1[1], "UUID should be marked as originating from the client"
    
    # A raw UUID viewed via a memoryview should be correctly extracted
    buf = BytesIO()
    buf.write(b"prefix")
    buf.write(ui1.bytes)
    buf.write(b"suffix")
    v = memoryview(buf.getvalue())
    uo1v = get_text_uuid([("_id", v[6:22])])
    assert uo1v[0] == str(ui1)
    assert uo1v[1], "UUID should be marked as originating from the client"
    
    ui2 = uuid.uuid4()
    uo2 = get_text_uuid([("_id", ui1.bytes), ("_id", ui2.bytes)])
    assert uo2[0] == str(ui1), "First ID should be selected"
    assert uo2[1], "UUID should be marked as originating from the client"
    
    uo3 = get_text_uuid([("_id", b"not a UUID"), ("_id", ui2.bytes)])
    assert uo3[0] == str(ui2), "Invalid garbage should be skipped"
    assert uo3[1], "UUID should be marked as originating from the client"
    
    uo4 = get_text_uuid([("_id", b"not a UUID")])
    uuid.UUID(hex=uo4[0])
    assert not uo4[1], "UUID should be marked as originating from the server"

def test_get_annotations():
    get_annotations = decision_api.get_annotations

    m1 = (b"abcdefgh",[])
    a1 = get_annotations(m1[0], m1[1])
    assert "con_text_uuid" in a1
    uuid.UUID(a1["con_text_uuid"])
    assert "con_is_client_uuid" in a1
    assert not a1["con_is_client_uuid"]
    assert "con_message_crc32" in a1
    
    u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
    m2 = (b"0123456789",[("_id",u.bytes)])
    a2 = get_annotations(m2[0], m2[1])
    assert "con_text_uuid" in a2
    assert uuid.UUID(a2["con_text_uuid"]) == u
    assert "con_is_client_uuid" in a2
    assert a2["con_is_client_uuid"]
    assert "con_message_crc32" in a2

@pytest.mark.asyncio
async def test_is_deemed_duplicate():
    get_annotations = decision_api.get_annotations
    is_deemed_duplicate = decision_api.is_deemed_duplicate

    config = {
        "db_type": "mock",
        "store_type": "mock",
        "store_primary_bucket": "b1",
        "store_backup_bucket": "b2",
    }
    db = database_api.DbFactory(config)
    st = store_api.StoreFactory(config)
    await db.connect()
    await st.connect()
    
    m1 = (b"abcdefgh",
          Metadata(topic="t1", partition=0, offset=0, timestamp=172, key="", headers=[], _raw=None))
    m2 = (b"abcdefgh",
          Metadata(topic="t1", partition=0, offset=1, timestamp=172, key="", headers=[], _raw=None))
    u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
    m3 = (b"somethingelse",
          Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None))
    
    async def store(payload, metadata, annotations):
        await st.store(payload, metadata, annotations)
        await db.insert(metadata, annotations)
        return annotations["con_text_uuid"]
    
    a1 = get_annotations(m1[0], m1[1].headers)
    assert not await is_deemed_duplicate(a1, m1[1], db, st), "With no messages stored, a message cannot be a duplicate"
    await store(m1[0], m1[1], a1)
    
    a2 = get_annotations(m2[0], m2[1].headers)
    assert await is_deemed_duplicate(a2, m2[1], db, st), "A message with the same body, timestamp, and topic is a duplicate"
    
    a3 = get_annotations(m3[0], m3[1].headers)
    assert not await is_deemed_duplicate(a3, m3[1], db, st), "A message with a new UUID is not a duplicate"
    await store(m3[0], m3[1], a3)
    
    assert await is_deemed_duplicate(a3, m3[1], db, st), "The same message again is a duplicate"