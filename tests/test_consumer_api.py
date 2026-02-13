import argparse
from hop.auth import Auth, SASLMethod, write_auth_data
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
		con = consumer_api.ConsumerFactory({"consumer_type": "mock"})
		mc.assert_called_once()
		hc.assert_not_called()
	
	with mock.patch("archive.consumer_api.Mock_consumer", mock.MagicMock()) as mc, \
	     mock.patch("archive.consumer_api.Hop_consumer", mock.MagicMock()) as hc:
		con = consumer_api.ConsumerFactory({"consumer_type": "hop"})
		mc.assert_not_called()
		hc.assert_called_once()
	
	with pytest.raises(RuntimeError) as err:
		st = consumer_api.ConsumerFactory({"consumer_type": "not a valid consumer type"})
	assert "not supported" in str(err)

def test_add_parser_options(tmpdir):
	parser = argparse.ArgumentParser()
	consumer_api.add_parser_options(parser)
	config = parser.parse_args(["--consumer-type", "hop",
                                "--kafka-hostname", "example.com",
                                "--kafka-port", "9092",
                                "--kafka-username", "archive",
                                "--kafka-groupname", "g1",
                                "--kafka-until-eos", "true",
                                "--kafka-local-auth", "/data/config",
                                "--kafka-aws-secret-name", "secret",
                                "--kafka-aws-secret-region", "eu-east-1",
                                "--kafka-vetoed-topics", "t1", "t2", "t3",
                                "--kafka-topic-refresh-interval", "300",
                                "--hopauth-api-url", "http://example.com/hopauth/api",
                                "--hopauth-username", "archive-user",
                                "--hopauth-local-auth", "/data/other_config",
                                "--hopauth-aws-secret-name", "secret2",
                                "--hopauth-aws-secret-region", "us-east-5"])
	assert config.consumer_type == "hop", "Consumer type should be set by the correct option"
	assert config.kafka_hostname == "example.com", "Kafka host should be set by the correct option"
	assert config.kafka_port == 9092, "Kafka port should be set by the correct option"
	assert config.kafka_username == "archive", "Kafka username should be set by the correct option"
	assert config.kafka_groupname == "g1", "Kafka group name should be set by the correct option"
	assert config.kafka_until_eos == True, "Kafka until EOS should be set by the correct option"
	assert config.kafka_local_auth == "/data/config", "Kafka local auth path name should be set by the correct option"
	assert config.kafka_aws_secret_name == "secret", "Kafka AWS secret name should be set by the correct option"
	assert config.kafka_aws_secret_region == "eu-east-1", "Kafka AWS secret region should be set by the correct option"
	assert config.kafka_vetoed_topics == ["t1", "t2", "t3"], "Kafka vetoed topics should be set by the correct option"
	assert config.kafka_topic_refresh_interval == 300, "Kafka topics refresh interval should be set by the correct option"
	assert config.hopauth_api_url == "http://example.com/hopauth/api", "Hopauth API URL should be set by the correct option"
	assert config.hopauth_username == "archive-user", "Hopauth API user should be set by the correct option"
	assert config.hopauth_local_auth == "/data/other_config", "Hopauth API local auth path should be set by the correct option"
	assert config.hopauth_aws_secret_name == "secret2", "Hopauth API AWS secret name should be set by the correct option"
	assert config.hopauth_aws_secret_region == "us-east-5", "Hopauth API AWS secret region should be set by the correct option"
    
	with temp_environ(CONSUMER_TYPE="mock", KAFKA_HOSTNAME="example.org", KAFKA_PORT="9093",
	                  KAFKA_USERNAME="archiver", KAFKA_GROUPNAME="g2", KAFKA_UNTIL_EOS="false",
	                  KAFKA_LOCAL_AUTH="/etc/auth", KAFKA_AWS_SECRET_NAME="Geheimnis",
	                  KAFKA_AWS_SECRET_REGION="eu-west-1", KAFKA_VETOED_TOPICS="t1 t2 t3",
	                  KAFKA_TOPIC_REFRESH_INTERVAL="90",
	                  HOPAUTH_API_URL="https://example.org/hopauth/api", HOPAUTH_USERNAME="user",
	                  HOPAUTH_LOCAL_AUTH="/etc/auth2", HOPAUTH_AWS_SECRET_NAME="shh",
	                  HOPAUTH_AWS_SECRET_REGION="us-east-6"):
		parser = argparse.ArgumentParser()
		consumer_api.add_parser_options(parser)
		config = parser.parse_args([])
		assert config.consumer_type == "mock", "Consumer type should be set by the correct environment variable"
		assert config.kafka_hostname == "example.org", "Kafka host should be set by the correct environment variable"
		assert config.kafka_port == 9093, "Kafka port should be set by the correct environment variable"
		assert config.kafka_username == "archiver", "Kafka username should be set by the correct environment variable"
		assert config.kafka_groupname == "g2", "Kafka group name should be set by the correct environment variable"
		assert config.kafka_until_eos == False, "Kafka until EOS should be set by the correct environment variable"
		assert config.kafka_local_auth == "/etc/auth", "Kafka local auth path should be set by the correct environment variable"
		assert config.kafka_aws_secret_name == "Geheimnis", "Kafka AWS secret name should be set by the correct environment variable"
		assert config.kafka_aws_secret_region == "eu-west-1", "Kafka AWS secretregion should be set by the correct environment variable"
		assert config.kafka_vetoed_topics == ["t1", "t2", "t3"], "Kafka vetoed topics should be set by the correct environment variable"
		assert config.kafka_topic_refresh_interval == 90, "Kafka topics refresh interval should be set by the correct environment variable"
		assert config.hopauth_api_url == "https://example.org/hopauth/api", "Hopauth API URL should be set by the correct environment variable"
		assert config.hopauth_username == "user", "Hopauth API user should be set by the correct environment variable"
		assert config.hopauth_local_auth == "/etc/auth2", "Hopauth API local auth path should be set by the correct environment variable"
		assert config.hopauth_aws_secret_name == "shh", "Hopauth API AWS secret name should be set by the correct environment variable"
		assert config.hopauth_aws_secret_region == "us-east-6", "Hopauth API AWS secret region should be set by the correct environment variable"

def get_consumer_test_config():
	return {
		"kafka_groupname": "archive-test",
		"kafka_until_eos": False,
		"kafka_username": "archive_test",
		"kafka_hostname": "example.com",
		"kafka_port": 9092,
		"kafka_vetoed_topics": ["ignore-topic-1", "ignore-topic-2"],
		"hopauth_api_url": "http://127.0.0.1/hopauth/api",
		"hopauth_username": "archive_test",
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
	with temp_environ(KAFKA_PASSWORD="test-pass", HOPAUTH_PASSWORD="test-api-pass"):
		hc = consumer_api.Hop_consumer(config)
		
		assert hc.kafka_auth.username == config["kafka_username"]
		assert hc.kafka_auth.password == os.environ["KAFKA_PASSWORD"]
		assert hc.hopauth_auth.username == config["hopauth_username"]
		assert hc.hopauth_auth.password == os.environ["HOPAUTH_PASSWORD"]
		assert hc.group_id.startswith(config["kafka_username"])
		assert hc.vetoed_topics == config["kafka_vetoed_topics"]

def test_hop_consumer_create_local(tmpdir):
	cred = Auth("foo", "bar")
	cred_path = tmpdir+"/auth.toml"
	write_auth_data(cred_path, [cred])
	config = {
		"kafka_local_auth": cred_path,
		"kafka_groupname": "*random*",
		"kafka_hostname": "example.com",
		"kafka_port": 9092,
		"hopauth_local_auth": cred_path,
		"hopauth_api_url": "http://127.0.0.1/hopauth/api",
	}
	hc = consumer_api.Hop_consumer(config)
	assert hc.kafka_auth == cred
	assert hc.hopauth_auth == cred

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
		"kafka_aws_secret_name": secret_name,
		"kafka_aws_secret_region": secret_region,
		"kafka_groupname": "*random*",
		"kafka_hostname": "example.com",
		"kafka_port": 9092,
		"hopauth_aws_secret_name": secret_name,
		"hopauth_aws_secret_region": secret_region,
		"hopauth_api_url": "http://127.0.0.1/hopauth/api",
	}
	with mock.patch("boto3.session.Session", mock.MagicMock(return_value=session)):
		hc = consumer_api.Hop_consumer(config)
		assert hc.kafka_auth == cred
		assert hc.hopauth_auth == cred
		session.client.assert_called_with(service_name="secretsmanager", region_name=secret_region)
		client.get_secret_value.assert_called_with(SecretId=secret_name)

def test_hop_consumer_create_secret_text_creds(tmpdir):
	cred = Auth("foo", "bar", method=SASLMethod.PLAIN)
	cred_data = {"SecretString": f'username="{cred.username}" password="{cred.password}"'}
	secret_name = "archive_cred"
	secret_region = "ac-pole-1"
	
	session = mock.MagicMock()
	client = mock.MagicMock()
	client.get_secret_value = mock.MagicMock(return_value=cred_data)
	session.client = mock.MagicMock(return_value=client)
	
	config = {
		"kafka_aws_secret_name": secret_name,
		"kafka_aws_secret_region": secret_region,
		"kafka_groupname": "*random*",
		"kafka_hostname": "example.com",
		"kafka_port": 9092,
		"hopauth_aws_secret_name": secret_name,
		"hopauth_aws_secret_region": secret_region,
		"hopauth_api_url": "http://127.0.0.1/hopauth/api",
	}
	with mock.patch("boto3.session.Session", mock.MagicMock(return_value=session)):
		hc = consumer_api.Hop_consumer(config)
		assert hc.kafka_auth == cred
		assert hc.hopauth_auth == cred
		session.client.assert_called_with(service_name="secretsmanager", region_name=secret_region)
		client.get_secret_value.assert_called_with(SecretId=secret_name)

def test_hop_consumer_create_secret_text_multi_creds(tmpdir):
	cred = Auth("baz", "quux", method=SASLMethod.PLAIN)
	cred_data = {"SecretString": 'username="foo" password="bar" '
	                             f'user_{cred.username}="{cred.password}" user_corge="grault"'}
	secret_name = "archive_cred"
	secret_region = "ac-pole-1"
	
	session = mock.MagicMock()
	client = mock.MagicMock()
	client.get_secret_value = mock.MagicMock(return_value=cred_data)
	session.client = mock.MagicMock(return_value=client)
	
	config = {
		"kafka_aws_secret_name": secret_name,
		"kafka_aws_secret_region": secret_region,
		"kafka_groupname": "*random*",
		"kafka_hostname": "example.com",
		"kafka_port": 9092,
		"kafka_username": "baz",
		"hopauth_aws_secret_name": secret_name,
		"hopauth_aws_secret_region": secret_region,
		"hopauth_api_url": "http://127.0.0.1/hopauth/api",
		"hopauth_username": "baz",
	}
	with mock.patch("boto3.session.Session", mock.MagicMock(return_value=session)):
		hc = consumer_api.Hop_consumer(config)
		assert hc.kafka_auth == cred
		assert hc.hopauth_auth == cred
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
	with temp_environ(KAFKA_PASSWORD="test-pass", HOPAUTH_PASSWORD="test-api-pass"):
		hc = consumer_api.Hop_consumer(config)
		assert hc.group_id.startswith(config["kafka_username"])
		assert "*random*" not in hc.group_id

class MockHttpResponse:
	def __init__(self, status, json):
		self.status_code = status
		self.json_data = json
	
	def ok(self):
		return self.status_code>=200 and self.status_code<=299
	
	def json(self):
		return self.json_data

def test_hop_consumer_refresh_url(tmpdir):
	config = get_consumer_test_config()
	topic_metadata = [{"name": "t1", "archivable": True}, {"name": "t2", "archivable": True}]
	with temp_environ(KAFKA_PASSWORD="test-pass", HOPAUTH_PASSWORD="test-api-pass"), \
	     mock.patch('time.time', MockClock(0)), \
	     mock.patch('archive.consumer_api.list_topics', TopicLister(["t1", "t2"])), \
	     mock.patch('requests.get', mock.Mock(return_value=MockHttpResponse(200, topic_metadata))), \
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
		topic_metadata.append({"name": "t3", "archivable": True})
		assert hc.refresh_url(), "Refresh should proceed at sufficiently late times"
		assert hc.url == "kafka://example.com:9092/t1,t3", \
		       "Refreshed URL should contain updated topics except ignored topics"
		time.time.adv_time(hc.refresh_interval)
		assert not hc.refresh_url(), "Refresh should do nothing when topics have not changed"
		time.time.adv_time(hc.refresh_interval)
		consumer_api.list_topics.set_topics(["t1","t2","t4"])
		topic_metadata.append({"name": "t4", "archivable": False})
		assert hc.refresh_url(), "Refresh should proceed at sufficiently late times"
		assert hc.url == "kafka://example.com:9092/t1,t2", \
		       "Refreshed URL should contain all archivable topics and no non-archivable topics"

class MockHopClient:
	def __init__(self, url_base, topics: TopicLister):
		self.messages = []
		self.url_base = url_base
		self.topics = topics
		self.was_closed = False
		self.marked_done = set()
	
	def read_raw(self, metadata: bool, autocommit: bool):
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
	topic_metadata = [{"name": "t1", "archivable": True}, {"name": "t2", "archivable": True},
	                  {"name": "t3", "archivable": True}, {"name": "t4", "archivable": False}]
	kafka = MockHopClient("kafka://example.com:9092/",topics)
	kafka.queue_message((b"data", Metadata("t1", 0, 0 , 123, "", [], None)))
	kafka.queue_message((b"atad", Metadata("t1", 0, 0 , 254, "", [], None)))
	with temp_environ(KAFKA_PASSWORD="test-pass", HOPAUTH_PASSWORD="test-api-pass"), \
	     mock.patch('time.time', MockClock(0)), \
	     mock.patch('requests.get', mock.Mock(return_value=MockHttpResponse(200, topic_metadata))), \
	     mock.patch('archive.consumer_api.list_topics', topics), \
	     mock.patch('archive.consumer_api.Stream', MockStreamFactory(kafka)), \
	     temp_wd(tmpdir):
		hc = consumer_api.Hop_consumer(config)
		time.time.adv_time(hc.refresh_interval)
		hc.connect()
		time.time.adv_time(hc.refresh_interval)
		iterator = hc.get_next()
		payload, metadata = iterator.__next__()
		assert payload == b"data"
		assert metadata.topic == "t1"
		assert metadata.timestamp == 123
		assert not kafka.was_closed
		
		hc.mark_done(metadata)
		assert kafka.is_marked_done(metadata)
		
		consumer_api.list_topics.set_topics(["t1","t3"])
		time.time.adv_time(hc.refresh_interval)
		payload, metadata = iterator.__next__()
		assert payload == b"atad"
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
	assert output[0][0] == b"data"
	assert output[0][1].topic == "t1"
	assert output[0][1].timestamp == 123
	assert output[1][0] == b"atad"
	assert output[1][1].topic == "t1"
	assert output[1][1].timestamp == 254
	
	for message, metadata in output:
		con.mark_done(metadata)
	
	con.close()