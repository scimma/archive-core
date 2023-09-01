import argparse
from hop.auth import Auth, write_auth_data
from hop.io import Metadata
import os
import pytest
import time
from unittest import mock

from archive import consumer_api

from conftest import temp_environ, temp_wd

pytest_plugins = ('pytest_asyncio',)

def test_ConsumerFactory():
	with mock.patch("archive.consumer_api.Mock_consumer", mock.MagicMock()) as mc, \
	     mock.patch("archive.consumer_api.Hop_consumer", mock.MagicMock()) as hc:
		con = consumer_api.ConsumerFactory({"hop_type": "mock"})
		mc.assert_called_once()
		hc.assert_not_called()
	
	with mock.patch("archive.consumer_api.Mock_consumer", mock.MagicMock()) as mc, \
	     mock.patch("archive.consumer_api.Hop_consumer", mock.MagicMock()) as hc:
		con = consumer_api.ConsumerFactory({"hop_type": "hop"})
		mc.assert_not_called()
		hc.assert_called_once()
	
	with pytest.raises(RuntimeError) as err:
		st = consumer_api.ConsumerFactory({"hop_type": "not a valid consumer type"})
	assert "not supported" in str(err)

def test_add_parser_options(tmpdir):
	parser = argparse.ArgumentParser()
	consumer_api.add_parser_options(parser)
	config = parser.parse_args(["--hop-type", "hop",
                                "--hop-hostname", "example.com",
                                "--hop-port", "9092",
                                "--hop-username", "archive",
                                "--hop-groupname", "g1",
                                "--hop-until-eos", "true",
                                "--hop-local-auth", "/data/config",
                                "--hop-aws-secret-name", "secret",
                                "--hop-aws-secret-region", "eu-east-1",
                                "--hop-vetoed-topics", "t1", "t2", "t3",
                                "--hop-topic-refresh-interval", "300"])
	assert config.hop_type == "hop", "Hop type should be set by the correct option"
	assert config.hop_hostname == "example.com", "Hop host should be set by the correct option"
	assert config.hop_port == 9092, "Hop port should be set by the correct option"
	assert config.hop_username == "archive", "Hop username should be set by the correct option"
	assert config.hop_groupname == "g1", "Hop group name should be set by the correct option"
	assert config.hop_until_eos == True, "Hop until EOS should be set by the correct option"
	assert config.hop_local_auth == "/data/config", "Hop local auth path name should be set by the correct option"
	assert config.hop_aws_secret_name == "secret", "Hop AWS secret name should be set by the correct option"
	assert config.hop_aws_secret_region == "eu-east-1", "Hop AWS secret region should be set by the correct option"
	assert config.hop_vetoed_topics == ["t1", "t2", "t3"], "Hop vetoed topics should be set by the correct option"
	assert config.hop_topic_refresh_interval == 300, "Hop topics refresh interval should be set by the correct option"
    
	with temp_environ(HOP_TYPE="mock", HOP_HOSTNAME="example.org", HOP_PORT="9093",
	                  HOP_USERNAME="archiver", HOP_GROUPNAME="g2", HOP_UNTIL_EOS="false",
	                  HOP_LOCAL_AUTH="/etc/auth", HOP_AWS_SECRET_NAME="Geheimnis",
	                  HOP_AWS_SECRET_REGION="eu-west-1", HOP_VETOED_TOPICS="t1 t2 t3",
	                  HOP_TOPIC_REFRESH_INTERVAL="90"):
		parser = argparse.ArgumentParser()
		consumer_api.add_parser_options(parser)
		config = parser.parse_args([])
		assert config.hop_type == "mock", "Hop type should be set by the correct environment variable"
		assert config.hop_hostname == "example.org", "Hop host should be set by the correct environment variable"
		assert config.hop_port == 9093, "Hop port should be set by the correct environment variable"
		assert config.hop_username == "archiver", "Hop username should be set by the correct environment variable"
		assert config.hop_groupname == "g2", "Hop group name should be set by the correct environment variable"
		assert config.hop_until_eos == False, "Hop until EOS should be set by the correct environment variable"
		assert config.hop_local_auth == "/etc/auth", "Hop local auth path should be set by the correct environment variable"
		assert config.hop_aws_secret_name == "Geheimnis", "Hop AWS secret name should be set by the correct environment variable"
		assert config.hop_aws_secret_region == "eu-west-1", "Hop AWS secretregion should be set by the correct environment variable"
		assert config.hop_vetoed_topics == ["t1", "t2", "t3"], "Hop vetoed topics should be set by the correct environment variable"
		assert config.hop_topic_refresh_interval == 90, "Hop topics refresh interval should be set by the correct environment variable"

def get_consumer_test_config():
	return {
		"hop_groupname": "archive-test",
		"hop_until_eos": False,
		"hop_username": "archive_test",
		"hop_hostname": "example.com",
		"hop_port": 9092,
		"hop_vetoed_topics": ["ignore-topic-1", "ignore-topic-2"],
	}

class MockClock:
	def __init__(self, initial_time = 0):
		self.current_time = initial_time
	
	def __call__(self):
		return self.current_time
	
	def set_time(self, time):
		self.current_time = time
	
	def adv_time(self, time_delta):
		self.current_time += time_delta
		
class TopicLister:
	def __init__(self, topics = []):
		self.topics = topics
	
	def set_topics(self, topics):
		self.topics = topics
		
	def __call__(self, url, auth):
		return {t: None for t in self.topics}

def test_hop_consumer_create():
	config = get_consumer_test_config()
	with temp_environ(HOP_PASSWORD="test-pass"):
		hc = consumer_api.Hop_consumer(config)
		
		assert hc.auth.username == config["hop_username"]
		assert hc.auth.password == os.environ["HOP_PASSWORD"]
		assert hc.group_id.startswith(config["hop_username"])
		assert hc.vetoed_topics == config["hop_vetoed_topics"]

def test_hop_consumer_create_local(tmpdir):
	cred = Auth("foo", "bar")
	cred_path = tmpdir+"/auth.toml"
	write_auth_data(cred_path, [cred])
	config = {
		"hop_local_auth": cred_path,
		"hop_groupname": "*random*",
		"hop_hostname": "example.com",
		"hop_port": 9092,
	}
	hc = consumer_api.Hop_consumer(config)
	assert hc.auth == cred

def test_hop_consumer_create_secret(tmpdir):
	cred = Auth("foo", "bar")
	cred_data = {"SecretString": '{'+f'"username": "{cred.username}", "password": "{cred.password}"'+'}'}
	secret_name = "archive_cred"
	secret_region = "ac-pole-1"
	
	session = mock.MagicMock()
	client = mock.MagicMock()
	client.get_secret_value = mock.MagicMock(return_value=cred_data)
	session.client = mock.MagicMock(return_value=client)
	
	config = {
		"hop_aws_secret_name": secret_name,
		"hop_aws_secret_region": secret_region,
		"hop_groupname": "*random*",
		"hop_hostname": "example.com",
		"hop_port": 9092,
	}
	with mock.patch("boto3.session.Session", mock.MagicMock(return_value=session)):
		hc = consumer_api.Hop_consumer(config)
		assert hc.auth == cred
		session.client.assert_called_with(service_name="secretsmanager", region_name=secret_region)
		client.get_secret_value.assert_called_with(SecretId=secret_name)

def test_hop_consumer_create_no_creds():
	config = get_consumer_test_config()
	with pytest.raises(RuntimeError) as err:
		hc = consumer_api.Hop_consumer(config)
	assert "credentials not configured" in str(err)

def test_hop_consumer_create_random_group():
	config = get_consumer_test_config()
	config["hop_groupname"] = "*random*"
	with temp_environ(HOP_PASSWORD="test-pass"):
		hc = consumer_api.Hop_consumer(config)
		assert hc.group_id.startswith(config["hop_username"])
		assert "*random*" not in hc.group_id

def test_hop_consumer_refresh_url(tmpdir):
	config = get_consumer_test_config()
	with temp_environ(HOP_PASSWORD="test-pass"), \
	     mock.patch('time.time', MockClock(0)), \
	     mock.patch('archive.consumer_api.list_topics', TopicLister(["t1", "t2"])), \
	     temp_wd(tmpdir):
		hc = consumer_api.Hop_consumer(config)
		assert time.time() == hc.last_last_refresh_time, "No time should yet have passed"
		assert not hc.refresh_url(), "Refresh should do nothing at time zero"
		time.time.adv_time(hc.refresh_interval)
		assert hc.refresh_url(), "Refresh should proceed at sufficiently late times"
		assert hc.url == "kafka://example.com:9092/t1,t2", "Refreshed URL should contain all topics"
		time.time.adv_time(hc.refresh_interval/2)
		assert not hc.refresh_url(), "Refresh should do nothing when called again soon"
		time.time.adv_time(hc.refresh_interval/2)
		consumer_api.list_topics.set_topics(["t1","t3","ignore-topic-1"])
		assert hc.refresh_url(), "Refresh should proceed at sufficiently late times"
		assert hc.url == "kafka://example.com:9092/t1,t3", \
		       "Refreshed URL should contain updated topics except ignored topics"
		time.time.adv_time(hc.refresh_interval)
		assert not hc.refresh_url(), "Refresh should do nothing when topics have not changed"

class MockMessage:
	def __init__(self, data: bytes):
		self.data = data
	
	def serialize(self):
		return {"format": "dummy", "content": self.data}

class MockHopClient:
	def __init__(self, url_base, topics: TopicLister):
		self.messages = []
		self.url_base = url_base
		self.topics = topics
		self.was_closed = False
		self.marked_done = set()
	
	def read(self, metadata: bool, autocommit: bool):
		assert metadata
		assert not autocommit
		return self
	
	def __iter__(self):
		return self
		
	def __next__(self):
		if not len(self.messages):
			raise StopIteration
		return self.messages.pop(0)
	
	def queue_message(self, m):
		self.messages.append(m)
		
	def close(self):
		self.was_closed = True
	
	def reset_closed(self):
		self.was_closed = False
	
	def expected_url(self):
		return f"{self.url_base}{','.join(self.topics(None, None).keys())}"
	
	def derive_id(self, metadata):
		return (metadata.topic, metadata.partition, metadata.offset, metadata.timestamp)
	
	def mark_done(self, metadata):
		self.marked_done.add(self.derive_id(metadata))
		
	def is_marked_done(self, metadata):
		return self.derive_id(metadata) in self.marked_done

class MockStream:
	def __init__(self, client):
		self.client = client

	def open(self, url, group_id):
		assert url == self.client.expected_url()
		return self.client
		
class MockStreamFactory:
	def __init__(self, client):
		self.client = client

	def __call__(self, **kwargs):
		return MockStream(self.client)

def test_hop_consumer_get_next(tmpdir):
	config = get_consumer_test_config()
	topics = TopicLister(["t1", "t2"])
	kafka = MockHopClient("kafka://example.com:9092/",topics)
	kafka.queue_message((MockMessage(b"data"), Metadata("t1", 0, 0 , 123, "", [], None)))
	kafka.queue_message((MockMessage(b"atad"), Metadata("t1", 0, 0 , 254, "", [], None)))
	with temp_environ(HOP_PASSWORD="test-pass"), \
	     mock.patch('time.time', MockClock(0)), \
	     mock.patch('archive.consumer_api.list_topics', topics), \
	     mock.patch('archive.consumer_api.Stream', MockStreamFactory(kafka)), \
	     temp_wd(tmpdir):
		hc = consumer_api.Hop_consumer(config)
		time.time.adv_time(hc.refresh_interval)
		hc.connect()
		time.time.adv_time(hc.refresh_interval)
		iterator = hc.get_next()
		payload, metadata = iterator.__next__()
		assert payload["content"] == b"data"
		assert metadata.topic == "t1"
		assert metadata.timestamp == 123
		assert not kafka.was_closed
		
		hc.mark_done(metadata)
		assert kafka.is_marked_done(metadata)
		
		consumer_api.list_topics.set_topics(["t1","t3"])
		time.time.adv_time(hc.refresh_interval)
		payload, metadata = iterator.__next__()
		assert payload["content"] == b"atad"
		assert metadata.topic == "t1"
		assert metadata.timestamp == 254
		assert kafka.was_closed
		kafka.reset_closed()
		
		hc.mark_done(metadata)
		assert kafka.is_marked_done(metadata)
	
		hc.close()
		assert kafka.was_closed

# These tests test test code, which is a bit pointless, but keeps it from cluttering up the coverage
# reports as being un-covered

def test_Base_consumer_unimplemented():
	con = consumer_api.Base_consumer({})
	with pytest.raises(NotImplementedError):
		con.get_next()

def test_Mock_consumer():
	con = consumer_api.Mock_consumer({})
	con.connect()
	
	con.queue(b"data", Metadata("t1", 0, 0 , 123, "", [], None))
	con.queue(b"atad", Metadata("t1", 0, 0 , 254, "", [], None))
	
	output = []
	for message, metadata in con.get_next():
		output.append((message, metadata))
		
	assert len(output) == 2
	assert output[0][0]["content"] == b"data"
	assert output[0][1].topic == "t1"
	assert output[0][1].timestamp == 123
	assert output[1][0]["content"] == b"atad"
	assert output[1][1].topic == "t1"
	assert output[1][1].timestamp == 254
	
	for message, metadata in output:
		con.mark_done(metadata)
	
	con.close()