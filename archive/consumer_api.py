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
import random
import logging
import time
import json
import hop
import os
import uuid
from hop.io import Stream, StartPosition, list_topics
from . import utility_api

def ConsumerFactory(config):
    type = config["hop_type"]
    #instantiate, then return consumer object of correct type.
    if type == "mock" : return Mock_consumer(config)
    if type == "hop"  : return Hop_consumer (config)
    raise RuntimeError(f"consumer {type} not supported")


def add_parser_options(parser):
    EnvDefault=utility_api.EnvDefault
    parser.add_argument("--hop-type", help="Type of consumer to use", type=str,
                        choices=["hop","mock"], default="hop", action=EnvDefault, envvar="HOP_TYPE")
    parser.add_argument("--hop-hostname", help="Hostname of the hop/Kafka broker", type=str,
                        action=EnvDefault, envvar="HOP_HOSTNAME", required=False)
    parser.add_argument("--hop-port", help="Port number on which to contact the hop/Kafka broker",
                        type=int, action=EnvDefault, envvar="HOP_PORT", default=9092,
                        required=False)
    parser.add_argument("--hop-username", help="Username of the hop/Kafka credential", type=str,
                        action=EnvDefault, envvar="HOP_USERNAME", required=False)
    parser.add_argument("--hop-groupname",
                        help="Kafka consumer group name to track reading progress", type=str,
                        action=EnvDefault, envvar="HOP_GROUPNAME", required=False)
    parser.add_argument("--hop-until-eos",
                        help="Whether to stop reading topics after consuming all current messages",
                        type=bool, default=False)
    parser.add_argument("--hop-local-auth",
                        help="Path to a local hop auth TOML file from which to read credentials",
                        type=str, action=EnvDefault, envvar="HOP_LOCAL_AUTH", required=False)
    parser.add_argument("--hop-aws-secret-name",
                        help="Name of an AWS secret from which to read hop credentials", type=str,
                        action=EnvDefault, envvar="HOP_AWS_SECRET_NAME", required=False)
    parser.add_argument("--hop-aws-secret-region",
                        help="Name of the AWS region in which to look for the AWS secret from which "
                        "to read hop credentials", type=str, action=EnvDefault, 
                        envvar="HOP_AWS_SECRET_REGION", default="us-west-2", required=False)
    parser.add_argument("--hop-vetoed-topics", help="Names of topics to skip reading", type=str, 
                        nargs='*', action=EnvDefault, envvar="HOP_VETOED_TOPICS",
                        default=["sys.heartbeat"])
    parser.add_argument("--hop-topic-refresh-interval",
                        help="Number of seconds to wait before rescanning topics to archive",
                        type=int, action=EnvDefault, envvar="HOP_TOPIC_REFRESH_INTERVAL",
                        default=600)


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
        self.messages.append(({"format": "dummy", "content": message}, metadata))


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
        self.groupname        = config["hop_groupname"]
        self.until_eos        = config.get("hop_until_eos", False)
        if "hop_username" in config and config["hop_username"] and "HOP_PASSWORD" in os.environ:
            auth  = [hop.auth.Auth(config["hop_username"], os.environ["HOP_PASSWORD"])]
        elif "hop_local_auth" in config and config["hop_local_auth"]:
            auth = hop.auth.load_auth(config["hop_local_auth"])
        elif "hop_aws_secret_name" in config and config["hop_aws_secret_name"] \
             and "hop_aws_secret_region" in config and config["hop_aws_secret_region"]:
            auth = self.auth_from_secret(config["hop_aws_secret_region"],
                                         config["hop_aws_secret_name"])
        else:
            raise RuntimeError("Hopskotch/Kafka credentials not configured")

        self.vetoed_topics    = config.get("hop_vetoed_topics", [])
        self.refresh_interval = config.get("hop_topic_refresh_interval", 600)
        self.last_last_refresh_time = 0

        broker_authority = f"{config['hop_hostname']}:{config['hop_port']}"
        self.auth = hop.auth.select_matching_auth(auth, broker_authority,
                                                  config.get("hop_username", None))
        self.base_url = f"kafka://{broker_authority}/"
        
        if self.groupname == '*random*':
            self.group_id = f"{self.auth.username}-{random.randint(0,10000)}"
        else:
            self.group_id = f"{self.auth.username}-{self.groupname}"

        self.n_recieved = 0
        self.client = None
    
    def auth_from_secret(self, secret_region: str, secret_name: str):
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=secret_region
        )
        resp = client.get_secret_value(
            SecretId=secret_name
        )['SecretString']
        resp = json.loads(resp)
        return [hop.auth.Auth(resp["username"], resp["password"])]

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
        # Read the available topics from the given broker
        # notice that we are not given "public topics" we
        # are given topics we are permitted to read. 
        topic_dict = list_topics(url=self.base_url, auth=self.auth)
        
        # TODO: Query the hop-auth API to find out which of these topics are supposed to be archived

        old_url = getattr(self, "url", None)
        # Concatinate the avilable topics with the broker address
        # omit vetoed topics
        topics = ','.join([t for t in sorted(topic_dict.keys()) if t not in self.vetoed_topics])
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
        stream = Stream(auth=self.auth, start_at=start_at, until_eos=self.until_eos)

        # Return the connection to the client as :class:"hop.io.Consumer" instance
        self.refresh_url()
        self.client = stream.open(url=self.url, group_id=self.group_id)
        logging.info(f"opening stream at {self.url} group: {self.group_id} startpos {start_at}")

    def get_next(self):
        keep_going = True
        while keep_going:
            keep_going = False
            self.connect()
            for message, metadata in self.client.read(metadata=True, autocommit=False):
                # TODO: It would be nice for hop to support a 'raw read' so we don't have to do this
                message = message.serialize()
            
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
