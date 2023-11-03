"""
Provide classes and API to the archive_ingest
relational databse

Access postgres in AWS. This class can be configured
via archive_ingest.toml and command line options. The
class can be configured  to access the production or
development versions of postgres via different
configurations.

DB credentials and configuation information
are stored in AWS secrets.

The DbFactory class supports choosing which class is
used at run-time. The framework supports a not-implemented
extention to SQLite.

"""

import boto3
from botocore.exceptions import ClientError
import sqlalchemy
from sqlalchemy.ext.asyncio import create_async_engine, create_async_pool_from_url
import psycopg
from psycopg_pool import AsyncConnectionPool
import aioboto3
import logging
from collections import namedtuple
from . import utility_api
import os

##################################
# "databases"
##################################


def DbFactory(config):
    """
    Factory to create Mock, MySQL, or AWS postgres DB objects
    """
    type = config["db_type"]
    if type == "mock" : return Mock_db(config)
    if type == "sql"  : return SQL_db(config)
    if type == "aws"  : return AWS_db(config)
    raise RuntimeError(f"database {type} not supported")


def add_parser_options(parser):
    EnvDefault=utility_api.EnvDefault
    parser.add_argument("--db-type", help="Type of database to use for metadata storage", type=str, choices=["sql","aws","mock"], action=EnvDefault, envvar="DB_TYPE", required=False)
    parser.add_argument("--db-host", help="Hostname for the metadata database", type=str, action=EnvDefault, envvar="DB_HOST", required=False)
    parser.add_argument("--db-port", help="Port for connecting to the metadata database", type=int, action=EnvDefault, envvar="DB_PORT", required=False)
    parser.add_argument("--db-name", help="Name of the metadata database", type=str, action=EnvDefault, envvar="DB_NAME", required=False)
    parser.add_argument("--db-username", help="Name of the user for the metadata database", type=str, action=EnvDefault, envvar="DB_USERNAME", required=False)
    parser.add_argument("--db-log-frequency", help="How often (in number of inserts) to log database insertions", type=int, action=EnvDefault, envvar="DB_LOG_FREQUENCY", default=100, required=False)
    parser.add_argument("--db-aws-secret-name", help="Name of an AWS secret from which to read database connection info", type=str, action=EnvDefault, envvar="DB_AWS_SECRET_NAME", required=False)
    parser.add_argument("--db-aws-region", help="Name of the AWS region in which to look for the database and AWS secret", type=str, action=EnvDefault, envvar="DB_AWS_REGION", default="us-west-2", required=False)
    parser.add_argument("--db-table", help="Name of the table for message metadata", type=str, action=EnvDefault, envvar="DB_TABLE", default="messages", required=False)


class Base_db:
    "Base class holding common methods"
    def __init__(self, config):
        self.config = config
        self.n_inserted = 0
        self.log_every = config.get("db_log_frequency",100)
        self.read_only = False

    async def launch_db_session(self):
        "lauch a shell level query session given credentials"
        raise NotImplementedError(f"Query_Session tool not supported for this database")

    async def make_schema(self):
        "no schema to make"

    def log(self):
        "log db informmation, but not too often"
        msg1 = f"inserted  {self.n_inserted} objects."
        if self.n_inserted < 10 :
            logging.info(msg1)
        elif self.n_inserted == 10:
            logging.info(msg1)
            logging.info(f"reverting to logging every {self.log_every}")
        elif self.n_inserted % self.log_every == 0:
            logging.info(msg1)

    async def insert(self, metadata, annotations):
        raise NotImplementedError

    MessageRecord = namedtuple("MessageRecord",
                               ["id", "topic", "timestamp", "uuid", "size", "key",
                                "bucket", "crc32", "is_client_uuid", "public",
                                "direct_upload", "message_crc32"])

    async def fetch(self, uuid) -> MessageRecord:
        raise NotImplementedError

    async def uuid_in_db(self, uuid):
        raise NotImplementedError

    async def exists_in_db(self, topic, timestamp, message_crc32):
        """
        Check whether a message with the given CRC sent at the given timestamp
        has previously been seen on the given topic.
        """
        raise NotImplementedError

    async def get_client_uuid_duplicates(self, limit: int = 1):
        """
        list UUIDs that are duplicates of an original UUID
    
        This routine  detects messages having UUIDs
        generated on the hop _client_.
        Some UUIDs may have multiple duplicates.
        only one duplicate UUID is returned.
    
        """
        raise NotImplementedError

    async def get_content_duplicates(self, limit: int = 1):
        """
        List messages which are probably duplicated based on their content checksums
        """
        raise NotImplementedError

    async def set_read_only(self):
        """
        Configure this database object to only perform reads, rejecting all
        modification operations.
        """
        self.read_only = True

    async def get_message_id(self, uuid):
        """
        Get the primary key associated with a message with the given UUID.
        This is a low-level interface intended mainly for testing.
        If duplicate UUIDs have gotten into the database, this will return
        only the id for one of them.
        """
        raise NotImplementedError
    
    async def get_message_locations(self, ids):
        """
        Get the location in the store of each of a set of messages spcified by
        UUID.
        
        Return: A sequence of tuples of bucket name, key where each mesage can
                be found in the data store.
        """
        raise NotImplementedError


class Mock_db(Base_db):
    """
    a mock DB that discards. -- support debug and devel.
    """
    def __init__(self, config):
        logging.info(f"Mock Database configured")
        super().__init__(config)
        self.data = {}
        self.next_id = 0
        self.connected = False

    async def connect(self):
        self.connected = True

    async def close(self):
        self.connected = False

    async def insert(self, metadata, annotations):
        if self.read_only:
            raise RuntimeError("This database object is set to read-only; insert is forbidden")
        assert self.connected
        
        value = Base_db.MessageRecord(
                  id = self.next_id,
                  topic = metadata.topic,
                  timestamp = metadata.timestamp,
                  uuid = annotations['con_text_uuid'],
                  size = annotations['size'],
                  key = annotations['key'],
                  bucket = annotations['bucket'],
                  crc32 = annotations['crc32'],
                  is_client_uuid = annotations['con_is_client_uuid'],
                  public = annotations['public'],
                  direct_upload = annotations['direct_upload'],
                  message_crc32 = annotations['con_message_crc32']
                  )
        self.data[value.id] = value
        self.next_id += 1

    async def fetch(self, uuid) -> Base_db.MessageRecord:
        assert self.connected
        for record in self.data.values():
            if record.uuid == uuid:
                return record
        return None

    async def uuid_in_db(self, uuid):
        assert self.connected
        for record in self.data.values():
            if record.uuid == uuid:
                return True
        return None

    async def exists_in_db(self, topic, timestamp, message_crc32):
        assert self.connected
        # This is not at all efficient, but should not be used for serious amounts of data
        for record in self.data.values():
            if record.topic == topic and record.timestamp == timestamp \
              and record.message_crc32 == message_crc32:
                return True
        return False

    async def get_message_id(self, uuid):
        """
        Get the primary key associated with a message with the given UUID.
        This is a low-level interface intended mainly for testing.
        If duplicate UUIDs have gotten into the database, this will return
        only the id for one of them.
        """
        assert self.connected
        for id, record in self.data.items():
            if record.uuid == uuid:
                return id
        return None

    async def get_message_locations(self, ids):
        """
        Get the location in the store of each of a set of messages spcified by
        UUID.
        
        Return: A sequence of tuples of bucket name, key where each mesage can
                be found in the data store.
        """
        assert self.connected
        results = []
        for id in ids:
            if id in self.data:
                record = self.data[id]
                results.append((record.bucket, record.key))
            # TODO do what if id is not known?
        return results

    async def get_message_records_for_time_range(self, topic: str, start_time: int, end_time: int, limit: int=10, offset: int=0):
        assert self.connected
        # This is not at all efficient, but should not be used for serious amounts of data
        results = []
        for record in sorted(self.data.values(), key=lambda r: r.timestamp):
            if record.topic == topic and \
              record.timestamp >= start_time and record.timestamp < end_time:
                if offset > 0:
                    offset -= 1
                else:
                    results.append(record)
                    if limit!=0 and len(results) == limit:
                        return results
        return results

class SQL_db(Base_db):
    def __init__(self, config):
        # allow some of these things to be None as a subclass may have its own ways of setting them
        super().__init__(config)
        self.user_name = config.get("db_username", None)
        self.db_name   = config.get("db_name", None)
        self.password  = os.environ.get("DB_PASSWORD", None)
        self.host      = config.get("db_host", None)
        self.port      = config.get("db_port", 5432)
        self.maxconn   = config.get("db_pool_size", 16)
        self.table_name = config.get("db_table", "messages")

    async def connect(self):
        "create  a session to postgres"
        if self.password is None:
            raise RuntimeError("SQL database password was not configured")
        if self.host is None:
            raise RuntimeError("SQL database host was not configured")
        self.engine = create_async_engine(
            url=f"postgresql+psycopg://{self.user_name}:{self.password}@{self.host}:{self.port}/{self.db_name}",
            pool_size  = self.maxconn,
        )
        self.db_meta = sqlalchemy.MetaData()
        Column = sqlalchemy.Column
        self.table = sqlalchemy.Table(
            self.table_name,
            self.db_meta,
            Column("id", sqlalchemy.dialects.postgresql.BIGINT, primary_key=True),
            Column("topic", sqlalchemy.dialects.postgresql.TEXT),
            Column("timestamp", sqlalchemy.dialects.postgresql.BIGINT),
            Column("uuid", sqlalchemy.dialects.postgresql.UUID),
            Column("size", sqlalchemy.dialects.postgresql.INTEGER),
            Column("key", sqlalchemy.dialects.postgresql.TEXT),
            Column("bucket", sqlalchemy.dialects.postgresql.TEXT),
            Column("crc32", sqlalchemy.dialects.postgresql.BIGINT),
            Column("is_client_uuid", sqlalchemy.dialects.postgresql.BOOLEAN),
            Column("public", sqlalchemy.dialects.postgresql.BOOLEAN),
            Column("direct_upload", sqlalchemy.dialects.postgresql.BOOLEAN),
            Column("message_crc32", sqlalchemy.dialects.postgresql.BIGINT),
        )

    async def close(self):
        await self.engine.dispose()

    async def make_schema(self):
        "Declare tables"
        ts_idx = sqlalchemy.Index(f"{self.table_name}_timestamp_idx", self.table.c.timestamp)
        topic_idx = sqlalchemy.Index(f"{self.table_name}_topic_idx", self.table.c.topic)
        key_idx = sqlalchemy.Index(f"{self.table_name}_key_idx", self.table.c.key)
        uuid_idx = sqlalchemy.Index(f"{self.table_name}_uuid_idx", self.table.c.uuid)
        async with self.engine.connect() as conn:
            await conn.execute(sqlalchemy.sql.ddl.CreateTable(self.table, if_not_exists=True))
            await conn.execute(sqlalchemy.sql.ddl.CreateIndex(ts_idx, if_not_exists=True))
            await conn.execute(sqlalchemy.sql.ddl.CreateIndex(topic_idx, if_not_exists=True))
            await conn.execute(sqlalchemy.sql.ddl.CreateIndex(key_idx, if_not_exists=True))
            await conn.execute(sqlalchemy.sql.ddl.CreateIndex(uuid_idx, if_not_exists=True))
            await conn.commit()

    async def insert(self, metadata, annotations):
        "insert one record into the DB"
        if self.read_only:
            raise RuntimeError("This database object is set to read-only; insert is forbidden")

        async with self.engine.connect() as conn:
            await conn.execute(
                self.table.insert().values(
                    topic = metadata.topic,
                    timestamp = metadata.timestamp,
                    uuid = annotations['con_text_uuid'],
                    size = annotations['size'],
                    key = annotations['key'],
                    bucket = annotations['bucket'],
                    crc32 = annotations['crc32'],
                    is_client_uuid = annotations['con_is_client_uuid'],
                    public = annotations['public'],
                    direct_upload = annotations['direct_upload'],
                    message_crc32 = annotations['con_message_crc32'],
                )
            )
            await conn.commit()
        self.n_inserted +=1
        self.log()

    async def fetch(self, uuid) -> Base_db.MessageRecord:
        """
        Fetch one mesage record (if it exists) from the DB.
        
        Returns: A tuple of all entries in the database row, or None if no
                 matching row was found.
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(self.table.select().where(self.table.c.uuid == uuid))
            record = result.first()
            if record is None:
                return None
            return Base_db.MessageRecord(**record._mapping)

    async def uuid_in_db(self, uuid):
        """
        Determine if this UUID is in the database
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(sqlalchemy.func.count()).select_from(self.table).where(self.table.c.uuid == uuid))
            return result.scalar() or False

    async def exists_in_db(self, topic, timestamp, message_crc32):
        """
        Check whether a message with the given CRC sent at the given timestamp
        has previously been seen on the given topic.
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(sqlalchemy.func.count())\
                                        .select_from(self.table)\
                                        .where((self.table.c.timestamp == timestamp) &
                                               (self.table.c.topic == topic) &
                                               (self.table.c.message_crc32 == message_crc32)))
            return result.scalar() or False

    async def get_client_uuid_duplicates(self, limit: int = 1):
        """
        list UUIDs that are duplicates of an original UUID
    
        This routine  detects messages having UUIDs
        generated on the hop _client_.
        Some UUIDs may have multiple duplicates.
        only one duplicate UUID is returned.
    
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(sqlalchemy.func.max(self.table.c.id),
                                                          self.table.c.uuid,
                                                          sqlalchemy.func.count())
                                        .select_from(self.table)
                                        .group_by(self.table.c.uuid)
                                        .having(sqlalchemy.func.count() > 1)
                                        .limit(limit)
                                        )
            return result.all()

    async def get_content_duplicates(self, limit: int = 1):
        """
        List messages which are probably duplicated based on their content checksums
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(sqlalchemy.func.max(self.table.c.id),
                                                          self.table.c.topic,
                                                          self.table.c.timestamp,
                                                          self.table.c.message_crc32,
                                                          sqlalchemy.func.count())
                                        .select_from(self.table)
                                        .group_by(self.table.c.topic,
                                                  self.table.c.timestamp,
                                                  self.table.c.message_crc32)
                                        .having(sqlalchemy.func.count() > 1)
                                        .limit(limit)
                                        )
            return result.all()

    async def set_read_only(self):
        """
        Configure this database object to only perform reads, rejecting all modification operations.
        """
        await super().set_read_only()

    def launch_db_session(self):
        "lauch a query_session tool for AWS databases"
        print (f"use password: {self.password}")
        import subprocess
        cmd = f"psql --dbname {self.DBName} --host={self.Address} --username={self.MasterUserName} --password"
        print (cmd)
        subprocess.run(cmd, shell=True)

    async def get_message_id(self, uuid):
        """
        Get the primary key associated with a message with the given UUID.
        This is a low-level interface intended mainly for testing.
        If duplicate UUIDs have gotten into the database, this will return
        only the id for one of them.
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(self.table.c.id)\
                                        .select_from(self.table)\
                                        .where(self.table.c.uuid == uuid))
            return result.scalar()

    async def get_message_locations(self, ids):
        """
        Get the location in the store of each of a set of messages specified by
        UUID.
        
        Return: A sequence of tuples of bucket name, key where each mesage can
                be found in the data store.
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(self.table.c.bucket, self.table.c.key)\
                                        .select_from(self.table)\
                                        .where(self.table.c.id.in_(ids)))
            return result.all()

    async def get_message_records_for_time_range(self, topic: str, start_time: int, end_time: int, limit: int=10, offset: int=0):
        async with self.engine.connect() as conn:
            result = await conn.execute(self.table.select()\
                                        .where((self.table.c.topic == topic) &
                                               (self.table.c.timestamp >= start_time) &
                                               (self.table.c.timestamp < end_time))
                                        .limit(limit).offset(offset))
            return [Base_db.MessageRecord(**record._mapping) for record in result.all()]


class AWS_db(SQL_db):
    """
    Logging to AWS postgres DBs
    """
    def __init__(self, config):
        super().__init__(config)
        # get these from the configuration file
        self.aws_db_secret_name = config["db_aws_secret_name"]
        self.aws_region_name    = config["db_aws_region"]

        # go off and get the real connections  information from AWS
        self.set_password_info()
        self.set_connect_info()
        logging.info(f"aws db name, secret, region: {self.db_name}, {self.aws_db_secret_name}, {self.aws_region_name } ")
        logging.info(f"aws database, user, port, address: {self.db_name}, {self.user_name}, {self.port} ,{self.host}")

    def set_password_info(self):
        "retrieve postgress password from its AWS secret"

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.aws_region_name
        )
        get_secret_value_response = client.get_secret_value(
            SecretId=self.aws_db_secret_name
        )
        password = get_secret_value_response['SecretString']
        self.password = password

    def set_connect_info(self):
        "set connection variabable from AWS"
        session = boto3.session.Session()
        client = session.client(
            service_name='rds',
            region_name=self.aws_region_name
        )
        result = client.describe_db_instances(
            DBInstanceIdentifier=self.db_name)
        result = result['DBInstances'][0]
        self.user_name = result['MasterUsername']
        self.db_name   = result['DBName']
        self.host      = result['Endpoint']['Address']
        self.port      = result['Endpoint']['Port']

    def get_logs(self):
        "return the latest postgres logs"
        session = boto3.session.Session()
        client = session.client(
            service_name='rds',
            region_name=self.aws_region_name
        )
        result = client.describe_db_log_files(DBInstanceIdentifier=self.db_name)
        all_logs = ""
        for idx in [-2,-1]:
            logfile = result['DescribeDBLogFiles'][idx]['LogFileName']
            log_text_result =  client.download_db_log_file_portion(
            DBInstanceIdentifier=self.db_name,
            LogFileName = logfile
            )
            all_logs += log_text_result["LogFileData"]
        return all_logs
