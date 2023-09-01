import argparse
import bson
from hop.io import Metadata
from io import BytesIO
import os
import pytest
from unittest import mock
import uuid

from archive import decision_api
from archive import store_api

from conftest import temp_environ, temp_minio

pytest_plugins = ('pytest_asyncio',)

def test_StoreFactory():
	with mock.patch("archive.store_api.Mock_store", mock.MagicMock()) as mst, \
	     mock.patch("archive.store_api.S3_store", mock.MagicMock()) as sst:
		st = store_api.StoreFactory({"store_type": "mock"})
		mst.assert_called_once()
		sst.assert_not_called()
	
	with mock.patch("archive.store_api.Mock_store", mock.MagicMock()) as mst, \
	     mock.patch("archive.store_api.S3_store", mock.MagicMock()) as sst:
		st = store_api.StoreFactory({"store_type": "S3"})
		mst.assert_not_called()
		sst.assert_called_once()
	
	with pytest.raises(RuntimeError) as err:
		st = store_api.StoreFactory({"store_type": "not a valid store type"})
	assert "not supported" in str(err)

def test_add_parser_options(tmpdir):
	parser = argparse.ArgumentParser()
	store_api.add_parser_options(parser)
	config = parser.parse_args(["--store-type", "S3",
                                "--store-primary-bucket", "pail",
                                "--store-backup-bucket", "tub",
                                "--store-endpoint-url", "http://foo.bar",
                                "--store-region-name", "us-east-5",
                                "--store-log-every", "12"])
	assert config.store_type == "S3", "Store type should be set by the correct option"
	assert config.store_primary_bucket == "pail", "Store primary bucket should be set by the correct option"
	assert config.store_backup_bucket == "tub", "Store backup bucket should be set by the correct option"
	assert config.store_endpoint_url == "http://foo.bar", "Store endpoint URL should be set by the correct option"
	assert config.store_region_name == "us-east-5", "Store region name should be set by the correct option"
	assert config.store_log_every == 12, "Store log frequency should be set by the correct option"
    
	with temp_environ(STORE_TYPE="mock", STORE_PRIMARY_BUCKET="tub", STORE_BACKUP_BUCKET="pail",
	                  STORE_ENDPOINT_URL="https://foo.bar", STORE_REGION_NAME="us-west-5", 
	                  STORE_LOG_EVERY="23"):
		parser = argparse.ArgumentParser()
		store_api.add_parser_options(parser)
		config = parser.parse_args([])
		assert config.store_type == "mock", "Store type should be set by the correct environment variable"
		assert config.store_primary_bucket == "tub", "Store primary bucket should be set by the correct environment variable"
		assert config.store_backup_bucket == "pail", "Store backup bucket should be set by the correct environment variable"
		assert config.store_endpoint_url == "https://foo.bar", "Store endpoint URL should be set by the correct environment variable"
		assert config.store_region_name == "us-west-5", "Store region name should be set by the correct environment variable"
		assert config.store_log_every == 23, "Store log frequency should be set by the correct environment variable"

@pytest.mark.asyncio
async def test_S3_store_startup(tmpdir):
	with temp_minio(tmpdir) as st_conf:
		st = store_api.S3_store(st_conf)
		await st.connect()
		obj_store = st_conf["__test_store_server"]
		assert obj_store.check_bucket_exists("archive"), "Primary bucket should be created"
		assert obj_store.check_bucket_exists("backup"),  "Backup bucket should be created"
		await st.close()

@pytest.mark.asyncio
async def test_S3_store_store_get(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = {"content":b"datadatadata"}
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	with temp_minio(tmpdir) as st_conf:
		st = store_api.S3_store(st_conf)
		await st.connect()
		
		await st.store(message, metadata, annotations)
		assert "size" in annotations
		assert "key" in annotations
		assert "bucket" in annotations
		assert "crc32" in annotations
		obj = await st.get_object(annotations["key"])
		assert obj is not None, "Message should be found"
		data = bson.loads(obj)
		assert data["message"]["content"] == message["content"]
		
		not_found = await st.get_object("some invalid key")
		assert not_found is None
		
		await st.close()

@pytest.mark.asyncio
async def test_S3_store_store_readonly(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = {"content":b"datadatadata"}
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	with temp_minio(tmpdir) as st_conf:
		st = store_api.S3_store(st_conf)
		await st.set_read_only()
		await st.connect()
		
		with pytest.raises(RuntimeError) as err:
			await st.store(message, metadata, annotations)
		assert "This store object is set to read-only; store is forbidden" in str(err)
		
		await st.close()

@pytest.mark.asyncio
async def test_S3_store_get_lazily(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = {"content":b"datadatadatadata"*64}
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	with temp_minio(tmpdir) as st_conf:
		st = store_api.S3_store(st_conf)
		await st.connect()
		
		await st.store(message, metadata, annotations)
		key = annotations["key"]
		result = await st.get_object_lazily(key)
		assert result
		output = BytesIO()
		async for chunk in result['Body']:
			output.write(chunk)
		data = bson.loads(output.getvalue())
		assert data["message"]["content"] == message["content"]
		
		result = await st.get_object_lazily("a-non-existent-key")
		assert result is None
		
		await st.close()

@pytest.mark.asyncio
async def test_S3_store_get_object_summary(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = {"content":b"datadatadatadata"*64}
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	with temp_minio(tmpdir) as st_conf:
		st = store_api.S3_store(st_conf)
		await st.connect()
		
		await st.store(message, metadata, annotations)
		key = annotations["key"]
		summary = await st.get_object_summary(key)
		assert "exists" in summary
		assert summary["exists"]
		assert "size" in summary
		assert summary["size"] == annotations["size"]
		
		summary = await st.get_object_summary("a-non-existent-key")
		assert "exists" in summary
		assert not summary["exists"]
		
		await st.close()
		
# These tests test test code, which is a bit pointless, but keeps it from cluttering up the coverage
# reports as being un-covered

@pytest.mark.asyncio
async def test_Mock_store_store_readonly(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = {"content":b"datadatadata"}
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	st = store_api.Mock_store({"store_primary_bucket": "a", "store_backup_bucket": "b"})
	await st.set_read_only()
	await st.connect()
	
	with pytest.raises(RuntimeError) as err:
		await st.store(message, metadata, annotations)
	assert "This store object is set to read-only; store is forbidden" in str(err)
	
	await st.close()
	

@pytest.mark.asyncio
async def test_Mock_store_deep_delete(tmpdir):
	u = uuid.UUID("01234567-aaaa-bbbb-cccc-0123456789de")
	message = {"content":b"datadatadata"}
	metadata = Metadata(topic="t1", partition=0, offset=2, timestamp=356, key="", headers=[("_id",u.bytes)], _raw=None)
	annotations = decision_api.get_annotations(message, metadata.headers)

	st = store_api.Mock_store({"store_primary_bucket": "a", "store_backup_bucket": "b"})
	await st.connect()
	await st.store(message, metadata, annotations)
	assert await st.get_object(annotations["key"]) is not None
	
	await st.deep_delete_object_from_store(annotations["key"])
	assert await st.get_object(annotations["key"]) is None
	
	await st.store(message, metadata, annotations)
	await st.set_read_only()
	
	with pytest.raises(RuntimeError) as err:
		await st.deep_delete_object_from_store(annotations["key"])
	assert "This store object is set to read-only; deep_delete_object_from_store is forbidden" in str(err)
	
	with pytest.raises(RuntimeError) as err:
		await st.deep_delete_object("a", annotations["key"])
	assert "This store object is set to read-only; deep_delete_object is forbidden" in str(err)
	
	await st.close()

def test_Base_store_unimplemented():
	bs = store_api.Base_store({"store_primary_bucket": "a", "store_backup_bucket": "b"})
	with pytest.raises(NotImplementedError):
		bs.get_object("akey")
	with pytest.raises(NotImplementedError):
		bs.get_object_summary("akey")
	with pytest.raises(NotImplementedError):
		bs.deep_delete_object_from_store("akey")