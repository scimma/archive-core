"""
Provide classes and API to sources of archive-ingest data.

There are two types archive-ingest classes

One class accesses hopskotch. This class can be configured
via archive_ingestx.toml. It can access the production or
development versions of hop via different configurations. Hop
credentials  are stored in AWS secrets.

The other class  is "mock" consumer useful for
development and test.

The ConsumerFactory class supports choosing which class is
used at run-time.


"""
##################################
#   Consumers
##################################

import boto3
import datetime
import hop
import json
import logging
import os
import random
import re
import requests
import time
from typing import Optional
from urllib.parse import urlparse
import uuid
from hop.io import Stream, StartPosition, list_topics
from hop import http_scram
from . import utility_api

def ConsumerFactory(config):
    type = config["consumer_type"]
    #instantiate, then return consumer object of correct type.
    if type == "mock" : return Mock_consumer(config)
    if type == "hop"  : return Hop_consumer (config)
    raise RuntimeError(f"consumer {type} not supported")


def add_parser_options(parser):
    EnvDefault=utility_api.EnvDefault
    parser.add_argument("--consumer-type", help="Type of consumer to use", type=str,
                        choices=["hop","mock"], default="hop", action=EnvDefault,
                        envvar="CONSUMER_TYPE")
    parser.add_argument("--kafka-hostname", help="Hostname of the hop/Kafka broker", type=str,
                        action=EnvDefault, envvar="KAFKA_HOSTNAME", required=False)
    parser.add_argument("--kafka-port", help="Port number on which to contact the hop/Kafka broker",
                        type=int, action=EnvDefault, envvar="KAFKA_PORT", default=9092,
                        required=False)
    parser.add_argument("--kafka-username",
                        help="Username of the credential to use for the kafka broker", type=str,
                        action=EnvDefault, envvar="KAFKA_USERNAME", required=False)
    parser.add_argument("--kafka-groupname",
                        help="Kafka consumer group name to track reading progress", type=str,
                        action=EnvDefault, envvar="KAFKA_GROUPNAME", required=False)
    parser.add_argument("--kafka-until-eos",
                        help="Whether to stop reading topics after consuming all current messages",
                        type=bool, default=False)
    parser.add_argument("--kafka-local-auth",
                        help="Path to a local hop auth TOML file from which to read credentials "
                        "for the kafka broker", type=str, action=EnvDefault,
                        envvar="KAFKA_LOCAL_AUTH", required=False)
    parser.add_argument("--kafka-aws-secret-name",
                        help="Name of an AWS secret from which to read kafka credentials", type=str,
                        action=EnvDefault, envvar="KAFKA_AWS_SECRET_NAME", required=False)
    parser.add_argument("--kafka-aws-secret-region",
                        help="Name of the AWS region in which to look for the AWS secret from which"
                        " to read hop credentials", type=str, action=EnvDefault,
                        envvar="KAFKA_AWS_SECRET_REGION", default="us-west-2", required=False)
    parser.add_argument("--kafka-vetoed-topics", help="Names of topics to skip reading", type=str,
                        nargs='*', action=EnvDefault, envvar="KAFKA_VETOED_TOPICS",
                        default=["sys.heartbeat"])
    parser.add_argument("--kafka-topic-refresh-interval",
                        help="Number of seconds to wait before rescanning topics to archive",
                        type=int, action=EnvDefault, envvar="KAFKA_TOPIC_REFRESH_INTERVAL",
                        default=600)
    parser.add_argument("--hopauth-api-url", help="Base URL for the hopauth API",
                        type=str, action=EnvDefault, envvar="HOPAUTH_API_URL", required=False)
    parser.add_argument("--hopauth-username",
                        help="Username of the credential to use for the hopauth API", type=str,
                        action=EnvDefault, envvar="HOPAUTH_USERNAME", required=False)
    parser.add_argument("--hopauth-local-auth",
                        help="Path to a local hop auth TOML file from which to read credentials "
                        "for the hopauth API", type=str, action=EnvDefault,
                        envvar="HOPAUTH_LOCAL_AUTH", required=False)
    parser.add_argument("--hopauth-aws-secret-name",
                        help="Name of an AWS secret from which to read hopauth API credentials",
                        type=str, action=EnvDefault, envvar="HOPAUTH_AWS_SECRET_NAME",
                        required=False)
    parser.add_argument("--hopauth-aws-secret-region",
                        help="Name of the AWS region in which to look for the AWS secret from which"
                        " to read hopauth API credentials", type=str, action=EnvDefault, 
                        envvar="HOPAUTH_AWS_SECRET_REGION", default="us-west-2", required=False)


class Base_consumer:
    "base class for common methods"

    def __init__(self, config):
        pass
    
    def connect(self):
        pass

    def get_next(self):
        raise NotImplementedError

    def mark_done(self, metadata):
        """
        Mark the message associated with the given metadata as fully processed.
        """
        pass

    def close(self):
        pass


class Mock_consumer(Base_consumer):
    def __init__(self, config):
        super().__init__(config)
        self.messages = []

    def get_next(self):
        for message in self.messages:
            yield message

    def queue(self, message: bytes, metadata):
        self.messages.append((message, metadata))


class Hop_consumer(Base_consumer):
    """
    A class to consume data from Hop
    
    The consumer reads messages from any stream
    authorized by its credentials, subject to a veto
    list which is read at startup. To accomodate newly
    defined topics, the list is refreshed by
    configurable timeout. Refreses are logged, and
    also written to the topics.log in the current working
    directory
    
    HOP credentials are stored as AWS secrets
    specifed in the configuration file.

    """
    def __init__(self, config):
        super().__init__(config)
        self.groupname        = config["kafka_groupname"]
        self.until_eos        = config.get("kafka_until_eos", False)
        
        broker_authority = f"{config['kafka_hostname']}:{config['kafka_port']}"
        self.base_url = f"kafka://{broker_authority}/"
        self.auth_api_url = config["hopauth_api_url"]
        
        if "kafka_username" in config and config["kafka_username"] \
          and "KAFKA_PASSWORD" in os.environ:
            self.kafka_auth = hop.auth.Auth(config["kafka_username"], os.environ["KAFKA_PASSWORD"])
        elif "kafka_local_auth" in config and config["kafka_local_auth"]:
            auth = hop.auth.load_auth(config["kafka_local_auth"])
            self.kafka_auth = hop.auth.select_matching_auth(auth, broker_authority,
                                                  config.get("kafka_username", None))
        elif "kafka_aws_secret_name" in config and config["kafka_aws_secret_name"] \
             and "kafka_aws_secret_region" in config and config["kafka_aws_secret_region"]:
            self.kafka_auth = self.auth_from_secret(config["kafka_aws_secret_region"],
                                                    config["kafka_aws_secret_name"],
                                                    config.get("kafka_username", None))
        else:
            raise RuntimeError("Kafka broker credentials not configured")
            
        if "hopauth_username" in config and config["hopauth_username"] \
          and "HOPAUTH_PASSWORD" in os.environ:
            self.hopauth_auth = hop.auth.Auth(config["hopauth_username"],
                                              os.environ["HOPAUTH_PASSWORD"])
        elif "hopauth_local_auth" in config and config["hopauth_local_auth"]:
            auth = hop.auth.load_auth(config["hopauth_local_auth"])
            self.hopauth_auth = hop.auth.select_matching_auth(auth,
                                                              urlparse(self.auth_api_url).netloc,
                                                              config.get("hopauth_username", None))
        elif "hopauth_aws_secret_name" in config and config["hopauth_aws_secret_name"] \
             and "hopauth_aws_secret_region" in config and config["hopauth_aws_secret_region"]:
            self.hopauth_auth = self.auth_from_secret(config["hopauth_aws_secret_region"],
                                                      config["hopauth_aws_secret_name"],
                                                      config.get("hopauth_username", None))
        else:
            raise RuntimeError("Hopauth API credentials not configured")

        self.vetoed_topics    = config.get("kafka_vetoed_topics", [])
        self.refresh_interval = config.get("kafka_topic_refresh_interval", 600)
        self.last_last_refresh_time = 0
        
        if self.groupname == '*random*':
            self.group_id = f"{self.kafka_auth.username}-{random.randint(0,10000)}"
        else:
            self.group_id = f"{self.kafka_auth.username}-{self.groupname}"

        self.n_recieved = 0
        self.client = None
    
    def auth_from_secret(self, secret_region: str, secret_name: str, username: str=""):
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=secret_region
        )
        resp = client.get_secret_value(
            SecretId=secret_name
        )['SecretString']
        try:
            data = json.loads(resp)
            return hop.auth.Auth(data["username"], data["password"])
        except json.JSONDecodeError:
            pass
        
        # if the data is not JSON try parsing it as a text config file
        chunks = resp.split(' ')
        creds = {}
        last_user=None
        for chunk in chunks:
            if match:=re.fullmatch('username="([^"]+)"', chunk):
                last_user=match[1]
            elif match:=re.fullmatch('password="([^"]+)"', chunk):
                if last_user is not None:
                    creds[last_user]=match[1]
                last_user=None
            elif match:=re.fullmatch('user_([^=]+)="([^"]+)"', chunk):
                creds[match[1]]=match[2]
                last_user=None
            else:
                raise RuntimeError("Text credential list parse error")
        if username and username in creds:
            return hop.auth.Auth(username, creds[username], method=hop.auth.SASLMethod.PLAIN)
        elif len(creds) == 1:
            username = next(iter(creds))
            return hop.auth.Auth(username, creds[username], method=hop.auth.SASLMethod.PLAIN)
        else:
            raise RuntimeError(f"Ambiguous text-format creential from secret {secret_name}")

    def refresh_url(self) -> bool:
        """
        initalize/refresh the list of topics to record PRN
        
        """
        # return if interval too small
        interval = time.time() - self.last_last_refresh_time
        if self.refresh_interval > interval:
            return False

        # interval exceeded -- reset time, and refresh topic list
        self.last_last_refresh_time = time.time()
        
        # Query the hopauth API for topic information to determine which are marked for archiving
        http_auth = http_scram.SCRAMAuth(self.hopauth_auth, shortcut=True)
        resp = requests.get(f"{self.auth_api_url}/v1/topics", auth=http_auth)
        if not resp.ok:
            raise RuntimeError(f"Failed to contact hopauth API: {resp.status_code}")
        topic_list = resp.json()
        topics_to_archive = []
        for topic_entry in topic_list:
            if "archivable" in topic_entry and topic_entry["archivable"] \
              and topic_entry["name"] not in self.vetoed_topics:
                topics_to_archive.append(topic_entry["name"])
        
        # Check that each topic to be archived is actually visible, and if so,
        # accumulate it onto the new URL.
        # Sort topic names to keep URL stable when the topic list does not change.
        topic_dict = list_topics(url=self.base_url, auth=self.kafka_auth)
        archivable_topics = []
        for topic_to_archive in sorted(topics_to_archive):
            if topic_to_archive in self.vetoed_topics:
                continue
            if topic_to_archive not in topic_dict:
                logging.error(f"Want to archive {topic_to_archive}, "
                              "but it is not accessible on the broker")
                continue;
            archivable_topics.append(topic_to_archive)

        old_url = getattr(self, "url", None)
        # Concatenate the avilable topics with the broker address
        # omit vetoed topics
        topics = ','.join(archivable_topics)
        self.url = f"{self.base_url}{topics}"
        changed: bool = self.url != old_url
        logging.info(f"Hop Url (re)configured: {self.url} excluding {self.vetoed_topics}")
        if changed:
            with open("topics.log","a") as f:
                f.write(f"{datetime.datetime.utcnow().isoformat()} {topics}\n")
        return changed

    def connect(self):
        "connect to HOP as a consumer (archiver)"
        if self.client:
            return  # if already connected, do nothing
        start_at = StartPosition.EARLIEST
        stream = Stream(auth=self.kafka_auth, start_at=start_at, until_eos=self.until_eos)

        # Return the connection to the client as :class:"hop.io.Consumer" instance
        self.refresh_url()
        self.client = stream.open(url=self.url, group_id=self.group_id)
        logging.info(f"opening stream at {self.url} group: {self.group_id} startpos {start_at}")

    def get_next(self):
        keep_going = True
        while keep_going:
            keep_going = False
            self.connect()
            for message, metadata in self.client.read_raw(metadata=True, autocommit=False):
                self.n_recieved += 1
                yield (message, metadata)
                url_changed = self.refresh_url()
                if url_changed:
                    logging.info("Input topics changed; closing and reopening Kafka stream")
                    keep_going = True
                    self.close()
                    break  # need to reconnect the consumer to adjust topics

    def mark_done(self, metadata):
        """
        mark that we are done processing a message, identified by its metadata,
        since autocommit=False
        """
        self.client.mark_done(metadata)

    def close(self):
        self.client.close()
        self.client = None
