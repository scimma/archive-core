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
from random import random
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
    logging.fatal(f"consumer {type} not supported")
    exit (1)


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

class Mock_message:
    """
    A class to define hold, and issue mock messages.

    The class variable Mock_message.message_list
    holds the list defined messages.

    Messages hold a message payload, and metadata,
    which include headers.    
    """
    message_list = []
    
    def __init__(self, usecase, timestamp=None, binary_uuid=None, is_client_uuid=True):
        "default to a  client-supplied UUID usecase"
        self.usecase = usecase
        self.message = os.urandom(50000)
        self.topic = "mock.topic"
        self.wrap_method =  hop.models.Blob
        self.headers = [("_mock_use_case", usecase)]
        
        # allow for simulating duplicates.
        self.timestamp = timestamp if timestamp else int(time.time()/1000) 
   
        # allow for client and server-side ,,,,
        # (pre hop lcient v1.8.0) simulatiosn
        self.is_client_uuid = is_client_uuid
        if self.is_client_uuid:
            self.binary_uuid = binary_uuid if binary_uuid else uuid.uuid4().bytes
            if self.is_client_uuid : self.headers.append (('_id',self.binary_uuid))
        #keep them all 
        Mock_message.message_list.append(self)
        
    def get_message(self):
        "generate, and return message body as if from HOP"
        return self.wrap_method(self.message).serialize()

    def get_metadata(self):
        "generate and return message body as if from HOP"
        metadata = hop.io.Metadata(topic=topic, partition=0, offset=0, timestamp=self.timestamp, 
                                   headers=self.headers)
        return metadata

    def get_all(self):
        "return messages, metadata as if from HOP"
        message = self.get_message()
        metadata = self.get_metadata()
        return message, metadata

        
class Mock_consumer(Base_consumer):
    """
    A mock consumer support generation
    of test use cases that cannot normally
    be generated by the test  producer.

    mock does this by producing data as if
    if was recieved from hop.

    Use cases supported:
    
    - use of hop client prior to hop ....
      ... client 1.8.0 (no _id in header.)
    - more-than-once delivery
    - replay of stream due to cursor reset.
    """

    def __init__(self, config):
        super().__init__(config)
        logging.info(f"Mock Consumer configured")
        # repeated recent  duplicate (e.g recived more than at least once) 
        # client_supplied UUID
        m = Mock_message(b'client-side uuid, recieved twice')
        _ = Mock_message(b'client-side uuid, recieved twice',
                         timestamp=m.timestamp, binary_uuid=m.binary_uuid)
        
        # repeated non-recent duplicate, (e.g cursor reset)
        # - client_supplied BINARY_UUID, both UUID's the same.
        # -timestamps are identical 
        t0 = time.time() - 24*60*60*10  #ten days old
        t0 = int(t0/1000)
        m = Mock_message(b'client-side uuid, recieved twice',timestamp = t0)
        _ = Mock_message(b'client-side uuid, recieved twice',timestamp = t0, binary_uuid=m.binary_uuid)

        # repeated  recent  duplicate (e.g recived more than at least once) 
        # - server_supplied UUID differ.
        # timestamps are identical
        m = Mock_message(b'server-side uuid, recieved twice',
                         is_client_uuid=False)
        m = Mock_message(b'server-side uuid, recieved twice',
                            is_client_uuid=False,
                            timestamp=m.timestamp)
        
        # repeated non-recent duplicate, (e.g cursor reset)
        # server_supplied UUID (e.g UUID's differ.)
        # timestamps are identical
        t0 = time.time() - 24*60*60*10  #ten days old
        t0 = int(t0/1000)
        m = Mock_message(b'server-side uuid, recieved twice',
                         is_client_uuid=False,
                         timestamp = t0)
        m = Mock_message(b'server-side uuid, recieved twice',
                         is_client_uuid=False,
                         timestamp = t0)

    def is_active(self):
        "has hop shut down?"
        if self.n_events > 0 :
            self.n_events = self.n_events-1
            return True
        else:
            return False

    def get_next(self):
        "get next mock message"
        for m in Mock_message.message_list:
            message, metadata = m.get_all()
            logging.info(f"moch data -- {metadata}  {annotations}")
            yield (message, metadata)

            

class Hop_consumer(Base_consumer):
    """
    A class to consume data from Hop
    
    The consumer reads messages from any stream
    authorized by its credentials, subject to a veto
    list which is read at startup. To accomodate newly
    defined topics, the list is refreshed by
    configurable timeout. Refreses are logged, and
    also written to the topics.log  in the currnet working
    directory

    Howeer, for testing the consumder will
    consume only from a configured test topic.
    This provides a defined environemt.
    
    HOP credentials are stored as AWS secrets
    specifed in the configuration file ...

    
    """
    def __init__(self, config):
        super().__init__(config)
        self.groupname        = config["hop_groupname"]
        self.until_eos        = config["hop_until_eos"]
        if config["hop_username"] and "HOP_PASSWORD" in os.environ:
            auth  = [hop.auth.Auth(oconfig["hop_username"], os.environ['HOP_PASSWORD'])]
        elif config["hop_local_auth"]:
            auth = hop.auth.load_auth(config["hop_local_auth"])
        elif config["hop_aws_secret_name"] and config["hop_aws_secret_region"]:
            auth = self.authorize_from_secret(config["hop_aws_secret_region"],
                                              config["hop_aws_secret_name"])
        else:
            raise RuntimeError("Hopskotch/Kafka credentials not configured")

        self.vetoed_topics    = config["hop_vetoed_topics"]
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
    
    def auth_from_secret(self, secret_region: str, scret_name: str):
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
            return

        # interval exceeded -- reset time, and refresh topic list
        self.last_last_refresh_time = time.time()
        # Read the available topics from the given broker
        # notice that we are not given "public topics" we
        # are given topics we are permitted to read. .seems
        # to include the private test topic.
        topic_dict = list_topics(url=self.base_url, auth=self.auth)

        old_url = getattr(self, "url", None)
        # Concatinate the avilable topics with the broker address
        # omit vetoed topics
        topics = ','.join([t for t in sorted(topic_dict.keys()) if t not in self.vetoed_topics])
        self.url = (f"{self.base_url}{topics}")
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

    def is_active(self):
        return True

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
