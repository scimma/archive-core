import argparse
from hop.io import Metadata
import pytest
import sqlalchemy
from unittest import mock
import uuid

from archive import database_api
from archive import decision_api
from archive import store_api

from conftest import temp_environ, temp_postgres

pytest_plugins = ('pytest_asyncio',)

def test_DbFactory():
	with mock.patch("archive.database_api.Mock_db", mock.MagicMock()) as mdb, \
	     mock.patch("archive.database_api.SQL_db", mock.MagicMock()) as sdb, \
	     mock.patch("archive.database_api.AWS_db", mock.MagicMock()) as adb:
		db = database_api.DbFactory({"db_type": "mock"})
		mdb.assert_called_once()
		sdb.assert_not_called()
		adb.assert_not_called()
	
	with mock.patch("archive.database_api.Mock_db", mock.MagicMock()) as mdb, \
	     mock.patch("archive.database_api.SQL_db", mock.MagicMock()) as sdb, \
	     mock.patch("archive.database_api.AWS_db", mock.MagicMock()) as adb:
		db = database_api.DbFactory({"db_type": "sql"})
		mdb.assert_not_called()
		sdb.assert_called_once()
		adb.assert_not_called()
	
	with mock.patch("archive.database_api.Mock_db", mock.MagicMock()) as mdb, \
	     mock.patch("archive.database_api.SQL_db", mock.MagicMock()) as sdb, \
	     mock.patch("archive.database_api.AWS_db", mock.MagicMock()) as adb:
		db = database_api.DbFactory({"db_type": "aws"})
		mdb.assert_not_called()
		sdb.assert_not_called()
		adb.assert_called_once()
	
	with pytest.raises(RuntimeError) as err:
		db = database_api.DbFactory({"db_type": "not a valid database type"})
	assert "not supported" in str(err)

def test_add_parser_options(tmpdir):
	parser = argparse.ArgumentParser()
	database_api.add_parser_options(parser)
	config = parser.parse_args(["--db-type", "sql",
                                "--db-host", "example.com",
                                "--db-port", "5432",
                                "--db-name", "my-database",
                                "--db-username", "bob",
                                "--db-log-frequency", "12",
                                "--db-aws-secret-name", "secret",
                                "--db-aws-region", "eu-east-1"])
	assert config.db_type == "sql", "DB type should be set by the correct option"
	assert config.db_host == "example.com", "DB host should be set by the correct option"
	assert config.db_port == 5432, "DB port should be set by the correct option"
	assert config.db_name == "my-database", "DB name should be set by the correct option"
	assert config.db_username == "bob", "DB username should be set by the correct option"
	assert config.db_log_frequency == 12, "DB log frequency should be set by the correct option"
	assert config.db_aws_secret_name == "secret", "DB AWS secret name should be set by the correct option"
	assert config.db_aws_region == "eu-east-1", "DB AWS region should be set by the correct option"
    
	with temp_environ(DB_TYPE="aws", DB_HOST="example.org", DB_PORT="5433",
	                  DB_NAME="the-database", DB_USERNAME="fred", DB_LOG_FREQUENCY="23",
	                  DB_AWS_SECRET_NAME="Geheimnis", DB_AWS_REGION="eu-west-1"):
		parser = argparse.ArgumentParser()
		database_api.add_parser_options(parser)
		config = parser.parse_args([])
		assert config.db_type == "aws", "DB type should be set by the correct environment variable"
		assert config.db_host == "example.org", "DB host should be set by the correct environment variable"
		assert config.db_port == 5433, "DB port should be set by the correct environment variable"
		assert config.db_name == "the-database", "DB name should be set by the correct environment variable"
		assert config.db_username == "fred", "DB username should be set by the correct environment variable"
		assert config.db_log_frequency == 23, "DB log frequency should be set by the correct environment variable"
		assert config.db_aws_secret_name == "Geheimnis", "DB AWS secret name should be set by the correct environment variable"
		assert config.db_aws_region == "eu-west-1", "DB AWS region should be set by the correct environment variable"

async def get_mock_store():
	st = store_api.StoreFactory({"store_type": "mock",
	                             "store_primary_bucket": "archive",
	                             "store_backup_bucket": "backup"})
	await st.connect()
	return st

@pytest.mark.asyncio
async def test_SQL_db_startup(tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.close()

@pytest.mark.asyncio
async def test_SQL_db_connect_no_password():
	db = database_api.SQL_db({"db_host": "a-host"})
	with pytest.raises(RuntimeError) as err:
		await db.connect()
	assert "SQL database password was not configured" in str(err)

@pytest.mark.asyncio
async def test_SQL_db_connect_no_host():
	with temp_environ(DB_PASSWORD="swordfish"):
		with pytest.raises(RuntimeError) as err:
			db = database_api.SQL_db({})
			await db.connect()
		assert "SQL database host was not configured" in str(err)

@pytest.mark.asyncio
async def test_SQL_db_create(tmpdir):
	expected_columns = {
		"id": "bigint",
		"topic": "text",
		"timestamp": "bigint",
		"uuid": "uuid",
		"size": "integer",
		"key": "text",
		"bucket": "text",
		"crc32": "bigint",
		"is_client_uuid": "boolean",
		"public": "boolean",
		"direct_upload": "boolean",
		"message_crc32": "bigint"
	}

	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		
		async with db.engine.connect() as conn:
			result = await conn.execute(sqlalchemy.text("SELECT column_name, data_type FROM information_schema.columns where table_name = 'messages'"))
			rows = result.all()
			print(rows)
			assert len(rows) == len(expected_columns), "table should have the expected number of columns"
			for cname, type in rows:
				assert cname in expected_columns, "Only expected columns should exist"
				assert type == expected_columns[cname], "Each column should have the expected type"
		
		await db.close()

@pytest.mark.asyncio
async def test_SQL_db_insert_readonly(tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		await db.set_read_only()
		with pytest.raises(RuntimeError) as err:		
			await db.insert("bar",{})
		assert "This database object is set to read-only; insert is forbidden" in str(err)
		async with db.engine.connect() as conn:
			result = (await conn.execute(sqlalchemy.text("SELECT * FROM messages"))).all()
		assert len(result) == 0, "No rows should be found"

@pytest.mark.asyncio
async def test_SQL_db_insert(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = b"datadatadata"
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	st = await get_mock_store()
	# we need to do this to get the last of the required annotations
	await st.store(message, metadata, annotations)
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		await db.insert(metadata, annotations)
		async with db.engine.connect() as conn:
			result = (await conn.execute(sqlalchemy.text("SELECT * FROM messages"))).all()
		assert len(result) == 1, "One row should be found"
		assert len(result[0]) == 12
		# with column is the id; we don't care about its value
		print(result[0])
		assert result[0][1] == metadata.topic
		assert result[0][2] == metadata.timestamp
		assert result[0][3] == u
		assert result[0][4] == annotations['size']
		assert result[0][5] == annotations['key']
		assert result[0][6] == annotations['bucket']
		assert result[0][7] == annotations['crc32']
		assert result[0][8] == annotations['con_is_client_uuid']
		assert result[0][9] == annotations['public']
		assert result[0][10] == annotations['direct_upload']
		assert result[0][11] == annotations['con_message_crc32']

@pytest.mark.asyncio
async def test_SQL_db_fetch(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = b"datadatadata"
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	st = await get_mock_store()
	# we need to do this to get the last of the required annotations
	await st.store(message, metadata, annotations)
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		result = await db.fetch(annotations['con_text_uuid'])
		assert result is None, "No result should be found"
		await db.insert(metadata, annotations)
		result = await db.fetch(annotations['con_text_uuid'])
		
		assert result.topic == metadata.topic
		assert result.timestamp == metadata.timestamp
		assert result.uuid == u
		assert result.size == annotations['size']
		assert result.key == annotations['key']
		assert result.bucket == annotations['bucket']
		assert result.crc32 == annotations['crc32']
		assert result.is_client_uuid == annotations['con_is_client_uuid']
		assert result.public == True
		assert result.direct_upload == False
		assert result.message_crc32 == annotations['con_message_crc32']

@pytest.mark.asyncio
async def test_SQL_db_uuid_in_db(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = b"datadatadata"
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	st = await get_mock_store()
	# we need to do this to get the last of the required annotations
	await st.store(message, metadata, annotations)
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		assert not await db.uuid_in_db(u), "Item should not yet be in the DB"
		await db.insert(metadata, annotations)
		assert await db.uuid_in_db(u), "Item should be in the DB"

@pytest.mark.asyncio
async def test_SQL_db_exists_in_db(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = b"datadatadata"
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	st = await get_mock_store()
	# we need to do this to get the last of the required annotations
	await st.store(message, metadata, annotations)
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		assert not await db.exists_in_db(metadata.topic, metadata.timestamp, annotations['con_message_crc32']), "Item should not yet be in the DB"
		await db.insert(metadata, annotations)
		assert await db.exists_in_db(metadata.topic, metadata.timestamp, annotations['con_message_crc32']), "Item should be in the DB"

@pytest.mark.asyncio
async def test_SQL_db_uuid_duplicates(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	m1 = b"datadatadata"
	meta1 = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	a1 = decision_api.get_annotations(m1, meta1.headers)
	
	m2 = b"otherdata"
	meta2 = Metadata(topic="t2", partition=0, offset=4, timestamp=19, key="", headers=[("_id",u.bytes)], _raw=None)
	a2 = decision_api.get_annotations(m2, meta2.headers)

	st = await get_mock_store()
	# we need to do this to get the last of the required annotations
	await st.store(m1, meta1, a1)
	await st.store(m2, meta2, a2)
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		
		result = await db.get_client_uuid_duplicates(limit=10)
		assert len(result) == 0, "No duplicate should be found"
		
		await db.insert(meta1, a1)
		result = await db.get_client_uuid_duplicates(limit=10)
		assert len(result) == 0, "No duplicate should be found"
		
		await db.insert(meta2, a2)
		result = await db.get_client_uuid_duplicates(limit=10)
		assert len(result) == 1, "A duplicate should be found"
		assert result[0][1] == u, "The duplicate should involve the correct UUID"

@pytest.mark.asyncio
async def test_SQL_db_content_duplicates(tmpdir):
	u1 = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	m1 = b"datadatadata"
	meta1 = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u1.bytes)], _raw=None)
	a1 = decision_api.get_annotations(m1, meta1.headers)
	
	u2 = uuid.UUID("76543210-ffff-eeee-dddd-9876543210ba")
	m2 = b"datadatadata"
	meta2 = Metadata(topic="t1", partition=0, offset=4, timestamp=356, key="", headers=[("_id",u2.bytes)], _raw=None)
	a2 = decision_api.get_annotations(m2, meta2.headers)

	st = await get_mock_store()
	# we need to do this to get the last of the required annotations
	await st.store(m1, meta1, a1)
	await st.store(m2, meta2, a2)
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		
		result = await db.get_content_duplicates(limit=10)
		assert len(result) == 0, "No duplicate should be found"
		
		await db.insert(meta1, a1)
		result = await db.get_content_duplicates(limit=10)
		assert len(result) == 0, "No duplicate should be found"
		
		await db.insert(meta2, a2)
		result = await db.get_content_duplicates(limit=10)
		assert len(result) == 1, "A duplicate should be found"
		assert result[0][1] == meta1.topic
		assert result[0][2] == meta1.timestamp
		assert result[0][3] == a1["con_message_crc32"]

@pytest.mark.asyncio
async def test_SQL_db_get_message_id(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = b"datadatadata"
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	st = await get_mock_store()
	# we need to do this to get the last of the required annotations
	await st.store(message, metadata, annotations)
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		id = await db.get_message_id(u)
		assert id is None, "Item not inserted should not be found"
		
		await db.insert(metadata, annotations)
		id = await db.get_message_id(u)
		assert id is not None, "Inserted item should be found"

@pytest.mark.asyncio
async def test_SQL_db_get_message_locations(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = b"datadatadata"
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	st = await get_mock_store()
	# we need to do this to get the last of the required annotations
	await st.store(message, metadata, annotations)
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		
		await db.insert(metadata, annotations)
		id = await db.get_message_id(u)
		result = await db.get_message_locations([id])
		assert len(result) == 1, "One result should be found"
		assert result[0][0] == annotations['bucket'], "Bucket name should be correct"
		assert result[0][1] == annotations['key'], "Object key should be correct"

@pytest.mark.asyncio
async def test_SQL_db_get_message_records_for_time_range(tmpdir):
	messages = []
	st = await get_mock_store()
	for i in range(0,10):
		ms = b"datadatadata"
		md = Metadata(topic="t1", partition=0, offset=i, timestamp=i, key="", headers=[("_id",uuid.uuid4().bytes)], _raw=None)
		an = decision_api.get_annotations(ms, md.headers)
		await st.store(ms, md, an)
		messages.append((ms,md,an))
	
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		
		for m in messages:
			await db.insert(m[1],m[2])
		
		r = await db.get_message_records_for_time_range("t1", start_time=4, end_time=7)
		assert len(r) == 3
		assert r[0].timestamp == 4
		assert r[0].uuid == uuid.UUID(messages[4][2]["con_text_uuid"])
		assert r[1].timestamp == 5
		assert r[1].uuid == uuid.UUID(messages[5][2]["con_text_uuid"])
		assert r[2].timestamp == 6
		assert r[2].uuid == uuid.UUID(messages[6][2]["con_text_uuid"])
		
		r = await db.get_message_records_for_time_range("t1", start_time=3, end_time=7, limit=2)
		assert len(r) == 2
		assert r[0].timestamp == 3
		assert r[0].uuid == uuid.UUID(messages[3][2]["con_text_uuid"])
		assert r[1].timestamp == 4
		assert r[1].uuid == uuid.UUID(messages[4][2]["con_text_uuid"])
		
		r = await db.get_message_records_for_time_range("t1", start_time=3, end_time=7, limit=2, offset=2)
		assert len(r) == 2
		assert r[0].timestamp == 5
		assert r[0].uuid == uuid.UUID(messages[5][2]["con_text_uuid"])
		assert r[1].timestamp == 6
		assert r[1].uuid == uuid.UUID(messages[6][2]["con_text_uuid"])
		
		r = await db.get_message_records_for_time_range("t1", start_time=12, end_time=14)
		assert len(r) == 0
		
		r = await db.get_message_records_for_time_range("t2", start_time=0, end_time=5)
		assert len(r) == 0

# These tests test test code, which is a bit pointless, but keeps it from cluttering up the coverage
# reports as being un-covered

@pytest.mark.asyncio
async def test_Base_db_unimlemented():
	db = database_api.Base_db({})
	
	with pytest.raises(NotImplementedError):
		await db.launch_db_session()
	
	with pytest.raises(NotImplementedError):
		await db.insert(None, None)
	
	with pytest.raises(NotImplementedError):
		await db.fetch(None)
	
	with pytest.raises(NotImplementedError):
		await db.uuid_in_db(None)
	
	with pytest.raises(NotImplementedError):
		await db.exists_in_db(None, None, None)
	
	with pytest.raises(NotImplementedError):
		await db.get_client_uuid_duplicates()
	
	with pytest.raises(NotImplementedError):
		await db.get_content_duplicates()
	
	with pytest.raises(NotImplementedError):
		await db.get_message_id(None)
	
	with pytest.raises(NotImplementedError):
		await db.get_message_locations(None)

@pytest.mark.asyncio
async def test_Mock_db_get_message_id():
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = b"datadatadata"
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	st = await get_mock_store()
	# we need to do this to get the last of the required annotations
	await st.store(message, metadata, annotations)
	db = database_api.Mock_db({})
	await db.connect()
	await db.make_schema()
	id = await db.get_message_id(str(u))
	assert id is None, "Item not inserted should not be found"
	
	await db.insert(metadata, annotations)
	id = await db.get_message_id(str(u))
	assert id is not None, "Inserted item should be found"

@pytest.mark.asyncio
async def test_Mock_db_insert_readonly():
	db = database_api.Mock_db({})
	await db.connect()
	await db.make_schema()
	await db.set_read_only()
	with pytest.raises(RuntimeError) as err:		
		await db.insert("bar",{})
	assert "This database object is set to read-only; insert is forbidden" in str(err)

@pytest.mark.asyncio
async def test_Mock_db_fetch():
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = b"datadatadata"
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	st = await get_mock_store()
	# we need to do this to get the last of the required annotations
	await st.store(message, metadata, annotations)
	db = database_api.Mock_db({})
	await db.connect()
	await db.make_schema()
	result = await db.fetch(annotations['con_text_uuid'])
	assert result is None, "No result should be found"
	await db.insert(metadata, annotations)
	result = await db.fetch(annotations['con_text_uuid'])
	
	assert result.topic == metadata.topic
	assert result.timestamp == metadata.timestamp
	assert result.uuid == annotations['con_text_uuid']
	assert result.size == annotations['size']
	assert result.key == annotations['key']
	assert result.bucket == annotations['bucket']
	assert result.crc32 == annotations['crc32']
	assert result.is_client_uuid == annotations['con_is_client_uuid']
	assert result.message_crc32 == annotations['con_message_crc32']