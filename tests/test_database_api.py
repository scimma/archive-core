import argparse
from contextlib import contextmanager
from hop.io import Metadata
import os
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
	                             "store_backup_bucket": "backup",
	                             "store_region_name": "eu-north-3"})
	await st.connect()
	return st

def generate_message(payload: bytes, topic: str, timestamp: int, public: bool=True, headers=[]):
	metadata = Metadata(topic=topic, partition=0, offset=0, timestamp=timestamp, key="", headers=headers, _raw=None)
	annotations = decision_api.get_annotations(payload, metadata.headers, public=public)
	annotations['size'] = len(payload)
	annotations['key'] = annotations["con_text_uuid"]
	annotations['bucket'] = "bucket"
	annotations['crc32'] = 0
	return payload, metadata, annotations

@pytest.mark.asyncio
async def test_SQL_db_startup(tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.close()

@contextmanager
def env_without(vars):
	original = dict(os.environ)
	for var in vars:
		if var in os.environ:
			del os.environ[var]
	try:
		yield
	finally:
		os.environ.update(original)

@pytest.mark.asyncio
async def test_SQL_db_connect_no_password():
	with env_without(["DB_PASSWORD"]):
		with pytest.raises(RuntimeError) as err:
			db = database_api.SQL_db({"db_host": "a-host"})
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
@pytest.mark.parametrize("db_class", (database_api.SQL_db, database_api.Mock_db))
async def test_SQL_db_get_topics_with_public_messages(db_class, tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = db_class(db_conf)
		await db.connect()
		await db.make_schema()
		
		p1, m1, a1 = generate_message(b"alert", topic="t1", timestamp=47, public=True)
		await db.insert(m1, a1)
		p2, m2, a2 = generate_message(b"another alert", topic="t1", timestamp=49, public=True)
		await db.insert(m2, a2)
		
		p3, m3, a3 = generate_message(b"secret", topic="t2", timestamp=22, public=False)
		await db.insert(m3, a3)
		p4, m4, a4 = generate_message(b"shared", topic="t2", timestamp=81, public=True)
		await db.insert(m4, a4)
		
		p5, m5, a5 = generate_message(b"private", topic="t3", timestamp=35, public=False)
		await db.insert(m5, a5)
		p4, m6, a6 = generate_message(b"proprietary", topic="t3", timestamp=48, public=False)
		await db.insert(m6, a6)
		
		pub_tops = await db.get_topics_with_public_messages()
		assert "t1" in pub_tops
		assert "t2" in pub_tops
		assert "t3" not in pub_tops

@pytest.mark.asyncio
@pytest.mark.parametrize("db_class", (database_api.SQL_db, database_api.Mock_db))
async def test_db_get_message_records_public(db_class, tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = db_class(db_conf)
		await db.connect()
		await db.make_schema()

		p1, m1, a1 = generate_message(b"alert", topic="t1", timestamp=47, public=True)
		await db.insert(m1, a1)
		p2, m2, a2 = generate_message(b"another alert", topic="t1", timestamp=49, public=True)
		await db.insert(m2, a2)
		
		p3, m3, a3 = generate_message(b"secret", topic="t2", timestamp=22, public=False)
		await db.insert(m3, a3)
		p4, m4, a4 = generate_message(b"shared", topic="t2", timestamp=81, public=True)
		await db.insert(m4, a4)
		
		p5, m5, a5 = generate_message(b"private", topic="t3", timestamp=35, public=False)
		await db.insert(m5, a5)
		p4, m6, a6 = generate_message(b"proprietary", topic="t3", timestamp=48, public=False)
		await db.insert(m6, a6)
		
		# with no topics explicitly selected, all public messages across all topics should be
		# selected
		results, n, p = await db.get_message_records(ascending=True)
		assert len(results) == 3
		# exploit time ordering to check that we got the right messages by their timestamps
		assert results[0].timestamp == 47
		assert results[1].timestamp == 49
		assert results[2].timestamp == 81
		assert all([r.public for r in results])
		
		# repeat, selecting only messages on one topic
		results, n, p = await db.get_message_records(topics_public=["t2"], ascending=True)
		# only the public message on the selected topics should be returned
		assert len(results) == 1
		assert results[0].timestamp == 81
		assert results[0].public
		
		# explicitly select multiple topics
		results, n, p = await db.get_message_records(topics_public=["t1", "t2"], ascending=True)
		assert len(results) == 3
		assert results[0].timestamp == 47
		assert results[1].timestamp == 49
		assert results[2].timestamp == 81
		assert all([r.public for r in results])
		
		# select a topic with no matching messages
		results, n, p = await db.get_message_records(topics_public=["t3"], ascending=True)
		assert len(results) == 0

@pytest.mark.asyncio
@pytest.mark.parametrize("db_class", (database_api.SQL_db, database_api.Mock_db))
async def test_SQL_db_get_message_records_full(db_class, tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = db_class(db_conf)
		await db.connect()
		await db.make_schema()

		p1, m1, a1 = generate_message(b"alert", topic="t1", timestamp=47, public=True)
		await db.insert(m1, a1)
		p2, m2, a2 = generate_message(b"another alert", topic="t1", timestamp=49, public=True)
		await db.insert(m2, a2)
		
		p3, m3, a3 = generate_message(b"secret", topic="t2", timestamp=22, public=False)
		await db.insert(m3, a3)
		p4, m4, a4 = generate_message(b"shared", topic="t2", timestamp=81, public=True)
		await db.insert(m4, a4)
		
		p5, m5, a5 = generate_message(b"private", topic="t3", timestamp=35, public=False)
		await db.insert(m5, a5)
		p4, m6, a6 = generate_message(b"proprietary", topic="t3", timestamp=48, public=False)
		await db.insert(m6, a6)
		
		# reading from a topic with full access should find both public and private messages
		results, n, p = await db.get_message_records(topics_full=["t2"], ascending=True)
		assert len(results) == 2
		# exploit time ordering to check that we got the right messages by their timestamps
		assert results[0].timestamp == 22
		assert not results[0].public
		assert results[1].timestamp == 81
		assert results[1].public
		
		# reading multiple topics should interleve messages in time order
		results, n, p = await db.get_message_records(topics_full=["t2", "t3"], ascending=True)
		assert len(results) == 4
		assert results[0].timestamp == 22
		assert not results[0].public
		assert results[0].topic == "t2"
		assert results[1].timestamp == 35
		assert not results[1].public
		assert results[1].topic == "t3"
		assert results[2].timestamp == 48
		assert not results[2].public
		assert results[2].topic == "t3"
		assert results[3].timestamp == 81
		assert results[3].public
		assert results[3].topic == "t2"
		
		# should be able to mix public and full access to different topics
		results, n, p = await db.get_message_records(topics_public=["t2"], topics_full=["t3"], ascending=True)
		assert len(results) == 3
		assert results[0].timestamp == 35
		assert not results[0].public
		assert results[0].topic == "t3"
		assert results[1].timestamp == 48
		assert not results[1].public
		assert results[1].topic == "t3"
		assert results[2].timestamp == 81
		assert results[2].public
		assert results[2].topic == "t2"
		

@pytest.mark.asyncio
@pytest.mark.parametrize("db_class", (database_api.SQL_db, database_api.Mock_db))
async def test_SQL_db_get_message_records_descending(db_class, tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = db_class(db_conf)
		await db.connect()
		await db.make_schema()

		p1, m1, a1 = generate_message(b"alert", topic="t1", timestamp=47, public=True)
		await db.insert(m1, a1)
		p2, m2, a2 = generate_message(b"another alert", topic="t1", timestamp=49, public=True)
		await db.insert(m2, a2)
		
		p3, m3, a3 = generate_message(b"secret", topic="t2", timestamp=22, public=False)
		await db.insert(m3, a3)
		p4, m4, a4 = generate_message(b"shared", topic="t2", timestamp=81, public=True)
		await db.insert(m4, a4)
		
		p5, m5, a5 = generate_message(b"private", topic="t3", timestamp=35, public=False)
		await db.insert(m5, a5)
		p4, m6, a6 = generate_message(b"proprietary", topic="t3", timestamp=48, public=False)
		await db.insert(m6, a6)
		
		# get all public messages, in descending order
		results, n, p = await db.get_message_records(ascending=False)
		assert len(results) == 3
		assert results[0].timestamp == 81
		assert results[1].timestamp == 49
		assert results[2].timestamp == 47
		assert all([r.public for r in results])
		
		results, n, p = await db.get_message_records(topics_public=["t1"], ascending=False)
		assert len(results) == 2
		assert results[0].timestamp == 49
		assert results[1].timestamp == 47
		assert all([r.public for r in results])
		
		results, n, p = await db.get_message_records(topics_full=["t1", "t3"], ascending=False)
		assert len(results) == 4
		assert results[0].timestamp == 49
		assert results[0].public
		assert results[0].topic == "t1"
		assert results[1].timestamp == 48
		assert not results[1].public
		assert results[1].topic == "t3"
		assert results[2].timestamp == 47
		assert results[2].public
		assert results[2].topic == "t1"
		assert results[3].timestamp == 35
		assert not results[3].public
		assert results[3].topic == "t3"

@pytest.mark.asyncio
@pytest.mark.parametrize("db_class", (database_api.SQL_db, database_api.Mock_db))
async def test_SQL_db_get_message_records_paging(db_class, tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = db_class(db_conf)
		await db.connect()
		await db.make_schema()
		
		for i in range(0,4):
			p, m, a = generate_message(b"data", topic="t1", timestamp=i, public=True)
			await db.insert(m, a)
		for i in range(4,8):
			p, m, a = generate_message(b"data", topic="t2", timestamp=i, public=False)
			await db.insert(m, a)
		for i in range(8,12):
			p, m, a = generate_message(b"data", topic="t1", timestamp=i, public=True)
			await db.insert(m, a)
		
		# get the first page of public messages
		results, n, p = await db.get_message_records(ascending=True, page_size=4)
		assert len(results) == 4
		assert n is not None
		assert p is None
		for r in results:
			assert r.topic == "t1"
			assert r.public
		assert [r.timestamp for r in results] == [0, 1, 2, 3]
		
		#get the next page of public messages
		results, n, p = await db.get_message_records(ascending=True, page_size=4, bookmark=n)
		assert len(results) == 4
		assert n is None
		assert p is not None
		for r in results:
			assert r.topic == "t1"
			assert r.public
		assert [r.timestamp for r in results] == [8, 9, 10, 11]
		
		# it should be possible to go back to the previous page
		# test with an increased page size to check that nothing goes wrong trying to get data from
		# before the beginning
		results, n, p = await db.get_message_records(ascending=True, page_size=5, bookmark=p)
		assert len(results) == 4
		assert n is not None
		assert p is None
		for r in results:
			assert r.topic == "t1"
			assert r.public
		assert [r.timestamp for r in results] == [0, 1, 2, 3]
		
		# get the first page of all messages
		results, n, p = await db.get_message_records(topics_full=["t1", "t2"], ascending=True, page_size=5)
		assert len(results) == 5
		assert n is not None
		assert p is None
		assert [r.timestamp for r in results] == [0, 1, 2, 3, 4]
		
		# get the second page of all messages
		results, n, p = await db.get_message_records(topics_full=["t1", "t2"], ascending=True, page_size=5, bookmark=n)
		assert len(results) == 5
		assert n is not None
		assert p is not None
		assert [r.timestamp for r in results] == [5, 6, 7, 8, 9]
		
		# get the third page of all messages
		results, n, p = await db.get_message_records(topics_full=["t1", "t2"], ascending=True, page_size=5, bookmark=n)
		assert len(results) == 2
		assert n is None
		assert p is not None
		assert [r.timestamp for r in results] == [10, 11]

@pytest.mark.asyncio
@pytest.mark.parametrize("db_class", (database_api.SQL_db, database_api.Mock_db))
async def test_SQL_db_get_message_records_time_range(db_class, tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = db_class(db_conf)
		await db.connect()
		await db.make_schema()
		
		for i in range(0,4):
			p, m, a = generate_message(b"data", topic="t1", timestamp=i, public=True)
			await db.insert(m, a)
		for i in range(4,8):
			p, m, a = generate_message(b"data", topic="t2", timestamp=i, public=False)
			await db.insert(m, a)
		for i in range(8,12):
			p, m, a = generate_message(b"data", topic="t1", timestamp=i, public=True)
			await db.insert(m, a)
		
		# all public messages
		results, n, p = await db.get_message_records(ascending=True)
		assert len(results) == 8
		assert [r.timestamp for r in results] == [0, 1, 2, 3, 8, 9, 10, 11]
		
		# all public messages after 2
		results, n, p = await db.get_message_records(ascending=True, start_time=2)
		assert len(results) == 6
		assert [r.timestamp for r in results] == [2, 3, 8, 9, 10, 11]
		
		# all public messages before 10
		results, n, p = await db.get_message_records(ascending=True, end_time=10)
		assert len(results) == 6
		assert [r.timestamp for r in results] == [0, 1, 2, 3, 8, 9]
		
		# all public messages between 2 and 10
		results, n, p = await db.get_message_records(ascending=True, start_time=2, end_time=10)
		assert len(results) == 4
		assert [r.timestamp for r in results] == [2, 3, 8, 9]
		
		# all messages
		results, n, p = await db.get_message_records(topics_full=["t1", "t2"], ascending=True)
		assert len(results) == 12
		assert [r.timestamp for r in results] == list(range(0, 12))
		
		# all messages after 2
		results, n, p = await db.get_message_records(topics_full=["t1", "t2"], ascending=True, start_time=2)
		assert len(results) == 10
		assert [r.timestamp for r in results] == list(range(2, 12))
		
		# all messages before 10
		results, n, p = await db.get_message_records(topics_full=["t1", "t2"], ascending=True, end_time=10)
		assert len(results) == 10
		assert [r.timestamp for r in results] == list(range(0, 10))
		
		# all messages between 2 and 10
		results, n, p = await db.get_message_records(topics_full=["t1", "t2"], ascending=True, start_time=2, end_time=10)
		assert len(results) == 8
		assert [r.timestamp for r in results] == list(range(2, 10))
		
		# make usre paging works sensibly with time limits
		results, n, p = await db.get_message_records(ascending=True, start_time=2, end_time=10, page_size=2)
		assert len(results) == 2
		assert [r.timestamp for r in results] == [2, 3]
		assert n is not None
		assert p is None
		results, n, p = await db.get_message_records(ascending=True, start_time=2, end_time=10, page_size=2, bookmark=n)
		assert len(results) == 2
		assert [r.timestamp for r in results] == [8, 9]
		assert n is None
		assert p is not None

@pytest.mark.asyncio
@pytest.mark.parametrize("db_class", (database_api.SQL_db, database_api.Mock_db))
async def test_SQL_db_count_message_records_public(db_class, tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = db_class(db_conf)
		await db.connect()
		await db.make_schema()

		p1, m1, a1 = generate_message(b"alert", topic="t1", timestamp=47, public=True)
		await db.insert(m1, a1)
		p2, m2, a2 = generate_message(b"another alert", topic="t1", timestamp=49, public=True)
		await db.insert(m2, a2)
		
		p3, m3, a3 = generate_message(b"secret", topic="t2", timestamp=22, public=False)
		await db.insert(m3, a3)
		p4, m4, a4 = generate_message(b"shared", topic="t2", timestamp=81, public=True)
		await db.insert(m4, a4)
		
		p5, m5, a5 = generate_message(b"private", topic="t3", timestamp=35, public=False)
		await db.insert(m5, a5)
		p4, m6, a6 = generate_message(b"proprietary", topic="t3", timestamp=48, public=False)
		await db.insert(m6, a6)
		
		# with no topics explicitly selected, all public messages across all topics should be
		# selected
		c = await db.count_message_records()
		assert c == 3
		
		# repeat, selecting only messages on one topic
		c = await db.count_message_records(topics_public=["t2"])
		# only the public message on the selected topics should be counted
		assert c == 1
		
		# explicitly select multiple topics
		c = await db.count_message_records(topics_public=["t1", "t2"])
		assert c == 3
		
		# explicitly select a topic with no matching messages
		c = await db.count_message_records(topics_public=["t3"])
		assert c == 0

@pytest.mark.asyncio
@pytest.mark.parametrize("db_class", (database_api.SQL_db, database_api.Mock_db))
async def test_SQL_db_count_message_records_full(db_class, tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = db_class(db_conf)
		await db.connect()
		await db.make_schema()

		p1, m1, a1 = generate_message(b"alert", topic="t1", timestamp=47, public=True)
		await db.insert(m1, a1)
		p2, m2, a2 = generate_message(b"another alert", topic="t1", timestamp=49, public=True)
		await db.insert(m2, a2)
		
		p3, m3, a3 = generate_message(b"secret", topic="t2", timestamp=22, public=False)
		await db.insert(m3, a3)
		p4, m4, a4 = generate_message(b"shared", topic="t2", timestamp=81, public=True)
		await db.insert(m4, a4)
		
		p5, m5, a5 = generate_message(b"private", topic="t3", timestamp=35, public=False)
		await db.insert(m5, a5)
		p4, m6, a6 = generate_message(b"proprietary", topic="t3", timestamp=48, public=False)
		await db.insert(m6, a6)
		
		# reading from a topic with full access should find both public and private messages
		c = await db.count_message_records(topics_full=["t2"])
		assert c == 2
		
		# reading multiple topics should interleve messages in time order
		c = await db.count_message_records(topics_full=["t2", "t3"])
		assert c == 4
		
		# should be able to mix public and full access to different topics
		c = await db.count_message_records(topics_public=["t2"], topics_full=["t3"])
		assert c == 3

@pytest.mark.asyncio
@pytest.mark.parametrize("db_class", (database_api.SQL_db, database_api.Mock_db))
async def test_SQL_db_count_message_records_time_range(db_class, tmpdir):
	with temp_postgres(tmpdir) as db_conf:
		db = db_class(db_conf)
		db = database_api.SQL_db(db_conf)
		await db.connect()
		await db.make_schema()
		
		for i in range(0,4):
			p, m, a = generate_message(b"data", topic="t1", timestamp=i, public=True)
			await db.insert(m, a)
		for i in range(4,8):
			p, m, a = generate_message(b"data", topic="t2", timestamp=i, public=False)
			await db.insert(m, a)
		for i in range(8,12):
			p, m, a = generate_message(b"data", topic="t1", timestamp=i, public=True)
			await db.insert(m, a)
		
		# all public messages
		c = await db.count_message_records()
		assert c == 8
		
		# all public messages after 2
		c = await db.count_message_records(start_time=2)
		assert c == 6
		
		# all public messages before 10
		c = await db.count_message_records(end_time=10)
		assert c == 6
		
		# all public messages between 2 and 10
		c = await db.count_message_records(start_time=2, end_time=10)
		assert c == 4
		
		# all messages
		c = await db.count_message_records(topics_full=["t1", "t2"])
		assert c == 12
		
		# all messages after 2
		c = await db.count_message_records(topics_full=["t1", "t2"], start_time=2)
		assert c == 10
		
		# all messages before 10
		c = await db.count_message_records(topics_full=["t1", "t2"], end_time=10)
		assert c == 10
		
		# all messages between 2 and 10
		c = await db.count_message_records(topics_full=["t1", "t2"], start_time=2, end_time=10)
		assert c == 8
		

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
	
	with pytest.raises(NotImplementedError):
		await db.get_topics_with_public_messages()
	
	with pytest.raises(NotImplementedError):
		await db.get_message_records()
	
	with pytest.raises(NotImplementedError):
		await db.count_message_records()

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
