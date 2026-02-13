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
import importlib.util
import sqlalchemy
bindparam = sqlalchemy.bindparam
from sqlalchemy.ext.asyncio import create_async_engine, create_async_pool_from_url, AsyncSession
from sqlakeyset.asyncio import select_page
import psycopg
from psycopg_pool import AsyncConnectionPool
import aioboto3
import logging
from dataclasses import dataclass
from . import utility_api
import os
from typing import Optional, List
from uuid import UUID

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

    async def insert(self, metadata, annotations, text_to_index=""):
        raise NotImplementedError

    # the fields in this class must match the columns defined in SQL_db.connect
    @dataclass
    class MessageRecord:
        id: int
        topic: str
        timestamp: int
        uuid: UUID
        size: int
        key: str
        bucket: str
        crc32: int
        is_client_uuid: bool
        public: bool
        direct_upload: bool
        message_crc32: int
        title: str
        sender: str
        media_type: str
        file_name: str

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

    async def get_topics_with_public_messages(self):
        """
        Get the names of all topics on which at least one public message is archived
        """
        raise NotImplementedError

    async def get_message_records(self, bookmark: Optional[str]=None, page_size: int=1024,
                                  ascending: bool=True,
                                  topics_public: Optional[List[str]]=None,
                                  topics_full: Optional[List[str]]=None,
                                  start_time: Optional[int]=None,
                                  end_time: Optional[int]=None,):
        """
        Get the records for messages satisfying specified criteria, with results split/batched into
        'pages'.
        Selecting messages by topic has some complexity: First, if neither topics_public nor
        topics_full is specified, the default is to select public messages from any topic. If either
        topic restriction argument is specified, no message is returned which is on a topic not
        specified by one of the two arguments. Both arguments may be specified at the same time to
        select a union of messages across multiple topics with different access levels.
        
        Args:
            bookmark: If not None, this must be a 'bookmark' string returned by a previous call, to
                      select another page of results.
            page_size: The maximum number of results to return fro this call; any further results
                       can be retrieved as subsequent 'pages'.
            ascending: Whether the reuslts should be sorted in ascending timestamp order.
            topics_public: If not None, only messages which are flagged as being public appearing on
                           these topics will be returned. Can be used at the same time as
                           topics_full.
            topics_full: If not None, any message appearing on one of these topics is a cadidate to
                         be returned.
            start_time: The beginning of the message timestamp range to select.
            end_time: The end of the message timestamp range to select. The range is half-open, so
                      messages with this exact timestamp will be excluded.
        Return: A tuple consisting of the results (a list of MessageRecords), a 'bookmark' which can
                be used to fetch the next page of results or None if there are no subsequent
                results, and a 'bookmark' for the previous page of results or None if there are no
                prior results.
        """
        raise NotImplementedError

    async def count_message_records(self,
                                    topics_public: Optional[List[str]]=None,
                                    topics_full: Optional[List[str]]=None,
                                    start_time: Optional[int]=None,
                                    end_time: Optional[int]=None,):
        """
        Count the numberof messages satisfying specified criteria, as they would be returned by
        get_message_records.
        
        Args:
            topics_public: If not None, only messages which are flagged as being public appearing on
                           these topics will be returned. Can be used at the same time as
                           topics_full.
            topics_full: If not None, any message appearing on one of these topics is a cadidate to
                         be returned.
            start_time: The beginning of the message timestamp range to select.
            end_time: The end of the message timestamp range to select. The range is half-open, so
                      messages with this exact timestamp will be excluded.
        Return: An integer count of messages
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
        self.text_index = []
        self.next_id = 0
        self.connected = False

    async def connect(self):
        self.connected = True

    async def close(self):
        self.connected = False

    async def insert(self, metadata, annotations, text_to_index=""):
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
                  message_crc32 = annotations['con_message_crc32'],
                  title = annotations['title'],
                  sender = annotations['sender'],
                  media_type = annotations['media_type'],
                  file_name = annotations['file_name'],
                  )
        self.data[value.id] = value
        self.next_id += 1
        self.text_index.append((text_to_index, annotations['con_text_uuid']))

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

    async def get_topics_with_public_messages(self):
        assert self.connected
        topics = set()
        for record in self.data.values():
            if record.public:
                topics.add(record.topic)
        return list(topics)

    def _generate_query_filter(self,
                               topics_public: Optional[List[str]]=None,
                               topics_full: Optional[List[str]]=None,
                               start_time: Optional[int]=None,
                               end_time: Optional[int]=None):
        def filter(record):
            if topics_public is not None or topics_full is not None:
                if topics_public is None:
                    if record.topic not in topics_full:
                        return False
                elif topics_full is None:
                    if not record.public or record.topic not in topics_public:
                        return False
                else:
                    if record.topic not in topics_full and (not record.public or record.topic not in topics_public):
                        return False
            elif not record.public:
                return False
            if start_time is not None and record.timestamp < start_time:
                return False
            if end_time is not None and record.timestamp >= end_time:
                return False
            return True
        return filter

    async def get_message_records(self, bookmark: Optional[str]=None, page_size: int=1024,
                                  ascending: bool=True,
                                  topics_public: Optional[List[str]]=None,
                                  topics_full: Optional[List[str]]=None,
                                  start_time: Optional[int]=None,
                                  end_time: Optional[int]=None,):
        assert self.connected
        filter = self._generate_query_filter(topics_public, topics_full, start_time, end_time)
        # This is not at all efficient, but should not be used for serious amounts of data
        matching = [r for r in self.data.values() if filter(r)]
        if len(matching) == 0:
            return [], None, None
        matching.sort(key=lambda r: (r.timestamp, r.id), reverse=not ascending)
        # apply pagination
        def make_bookmark(dir, ts, i):
            # This is not quite the format used by sqlakeyset, but we don't need as much generality,
            # nor is interoperability needed, and this is simpler to parse.
            return f"{dir}{ts}~{i}"
        if bookmark is not None:
            assert len(bookmark) >= 4
            direction = bookmark[0]
            assert direction in ('<', '>')
            ts, i = tuple(int(s) for s in bookmark[1:].split("~"))
            if direction == '>':
                startIdx = 0
                while startIdx < len(matching) and matching[startIdx].timestamp < ts or \
                  (matching[startIdx].timestamp == ts and matching[startIdx].id <= i):
                    startIdx += 1
                if startIdx >= len(matching):
                    return [], None, make_bookmark('<', matching[0].timestamp, matching[0].id+1)
                endIdx = startIdx + page_size
                if endIdx > len(matching):
                    endIdx = len(matching)
            else: # '<'
                endIdx = 0
                while endIdx < len(matching) and matching[endIdx].timestamp < ts or \
                  (matching[endIdx].timestamp == ts and matching[endIdx].id < i):
                    endIdx += 1
                if endIdx == 0:
                    return [], make_bookmark('>', matching[0].timestamp, matching[0].id-1), None
                startIdx = endIdx - page_size
                if startIdx < 0:
                    startIdx = 0
        else:
            startIdx = 0
            endIdx = startIdx + page_size
            if endIdx > len(matching):
                endIdx = len(matching)
        if endIdx < len(matching):
            next = make_bookmark('>', matching[endIdx-1].timestamp, matching[endIdx-1].id)
        else:
            next = None
        if startIdx > 0:
            prev = make_bookmark('<', matching[startIdx].timestamp, matching[startIdx].id)
        else:
            prev = None
        return matching[startIdx:endIdx], next, prev

    async def count_message_records(self,
                                    topics_public: Optional[List[str]]=None,
                                    topics_full: Optional[List[str]]=None,
                                    start_time: Optional[int]=None,
                                    end_time: Optional[int]=None,):
        assert self.connected
        filter = self._generate_query_filter(topics_public, topics_full, start_time, end_time)
        # This is not at all efficient, but should not be used for serious amounts of data
        count = 0
        for record in self.data.values():
            if filter(record):
                count += 1
        return count

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
        self.messages_table_name = config.get("db_table", "messages")

    async def connect(self):
        "create  a session to postgres"
        if self.password is None:
            raise RuntimeError("SQL database password was not configured")
        if self.host is None:
            raise RuntimeError("SQL database host was not configured")
        self.engine = create_async_engine(
            url=f"postgresql+psycopg://{self.user_name}:{self.password}@{self.host}:{self.port}/{self.db_name}",
            pool_size  = self.maxconn,
            echo = True,
        )
        self.db_meta = sqlalchemy.MetaData()
        Column = sqlalchemy.Column
        # the columns defined in here must match the fields in Base_db.MessageRecord
        self.messages_table = sqlalchemy.Table(
            self.messages_table_name,
            self.db_meta,
            Column("id", sqlalchemy.dialects.postgresql.BIGINT, primary_key=True),
            Column("topic", sqlalchemy.dialects.postgresql.TEXT, nullable=False),
            Column("timestamp", sqlalchemy.dialects.postgresql.BIGINT, nullable=False),
            Column("uuid", sqlalchemy.dialects.postgresql.UUID, nullable=False),
            Column("size", sqlalchemy.dialects.postgresql.INTEGER, nullable=False),
            Column("key", sqlalchemy.dialects.postgresql.TEXT, nullable=False),
            Column("bucket", sqlalchemy.dialects.postgresql.TEXT, nullable=False),
            Column("crc32", sqlalchemy.dialects.postgresql.BIGINT, nullable=False),
            Column("is_client_uuid", sqlalchemy.dialects.postgresql.BOOLEAN, nullable=False),
            Column("public", sqlalchemy.dialects.postgresql.BOOLEAN, nullable=False),
            Column("direct_upload", sqlalchemy.dialects.postgresql.BOOLEAN, nullable=False),
            Column("message_crc32", sqlalchemy.dialects.postgresql.BIGINT, nullable=False),
            Column("title", sqlalchemy.dialects.postgresql.TEXT, nullable=False, default=""),
            Column("sender", sqlalchemy.dialects.postgresql.TEXT, nullable=False, default=""),
            Column("media_type", sqlalchemy.dialects.postgresql.TEXT, nullable=False, default=""),
            Column("file_name", sqlalchemy.dialects.postgresql.TEXT, nullable=False, default=""),
        )
        self.ts_table = sqlalchemy.Table(
            "message_ts",
            self.db_meta,
            Column("uuid", sqlalchemy.dialects.postgresql.UUID, sqlalchemy.ForeignKey("user.user_id"), nullable=False),
            Column("text_data", sqlalchemy.dialects.postgresql.TSVECTOR, nullable=False),
            Column("text_fully_indexed", sqlalchemy.dialects.postgresql.BOOLEAN, nullable=False),
        )
        self.topic_table = sqlalchemy.Table(
            "topics",
            self.db_meta,
            Column("topic", sqlalchemy.dialects.postgresql.TEXT, nullable=False),
            Column("e_timestamp", sqlalchemy.dialects.postgresql.BIGINT, nullable=False),
            Column("l_timestamp", sqlalchemy.dialects.postgresql.BIGINT, nullable=False),
            Column("n_messages", sqlalchemy.dialects.postgresql.BIGINT, nullable=False),
        )

    async def close(self):
        await self.engine.dispose()

    async def make_schema(self):
        mod_path = os.path.dirname(importlib.util.find_spec("archive").origin)
        migration_dir = os.path.join(mod_path, "migrations")
        async with self.engine.connect() as conn:
            filenames = os.listdir(migration_dir)
            filenames.sort()
            for filename in filenames:
                logging.info(f"Running migration {filename}")
                if not filename.endswith(".sql"):
                    continue
                with open(os.path.join(migration_dir, filename), 'r') as f:
                    # escape '%' characters to make code survive sqlalchemy using the % operator on it
                    ddl = sqlalchemy.DDL(f.read().replace("%", "%%"))
                await conn.execute(ddl)

    async def insert(self, metadata, annotations, text_to_index=""):
        "insert one record into the DB"
        if self.read_only:
            raise RuntimeError("This database object is set to read-only; insert is forbidden")

        async with self.engine.connect() as conn:
            await conn.execute(
                self.messages_table.insert().values(
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
                    title = annotations['title'],
                    sender = annotations['sender'],
                    media_type = annotations['media_type'],
                    file_name = annotations['file_name'],
                )
            )
            await conn.execute(
                self.ts_table.insert().values(
                    uuid = annotations['con_text_uuid'],
                    text_data = sqlalchemy.func.to_tsvector(text_to_index),
                    text_fully_indexed = annotations.get("text_fully_indexed", False),
                )
            )
            await conn.commit()
        self.n_inserted +=1
        self.log()

    async def set_indexed_text(self, message_uuid, text_to_index, fully_indexed, is_update: bool=True):
        operation = sqlalchemy.insert if not is_update else sqlalchemy.update
        async with self.engine.connect() as conn:
            stmt = operation(self.ts_table).values(
                uuid = message_uuid,
                text_data = sqlalchemy.func.to_tsvector(text_to_index),
                text_fully_indexed = fully_indexed,
            )
            if is_update:
                stmt = stmt.where(self.ts_table.c.uuid == message_uuid)
            result = await conn.execute(stmt)
            await conn.commit()

    async def fetch(self, uuid) -> Base_db.MessageRecord:
        """
        Fetch one mesage record (if it exists) from the DB.
        
        Returns: A tuple of all entries in the database row, or None if no
                 matching row was found.
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(self.messages_table.select()\
                                        .where(self.messages_table.c.uuid == bindparam("uuid")),
                                        {"uuid":uuid})
            record = result.first()
            if record is None:
                return None
            return Base_db.MessageRecord(**record._mapping)

    async def uuid_in_db(self, uuid):
        """
        Determine if this UUID is in the database
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(sqlalchemy.func.count())\
                                        .select_from(self.messages_table)\
                                        .where(self.messages_table.c.uuid == bindparam("uuid")),
                                        {"uuid":uuid})
            return result.scalar() or False

    async def exists_in_db(self, topic, timestamp, message_crc32):
        """
        Check whether a message with the given CRC sent at the given timestamp
        has previously been seen on the given topic.
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(sqlalchemy.func.count())\
                                        .select_from(self.messages_table)\
                                        .where((self.messages_table.c.timestamp == bindparam("timestamp")) &
                                               (self.messages_table.c.topic == bindparam("topic")) &
                                               (self.messages_table.c.message_crc32 == bindparam("crc32"))),
                                        {"timestamp":timestamp,
                                         "topic":topic,
                                         "crc32":message_crc32})
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
            result = await conn.execute(sqlalchemy.select(sqlalchemy.func.max(self.messages_table.c.id),
                                                          self.messages_table.c.uuid,
                                                          sqlalchemy.func.count())
                                        .select_from(self.messages_table)
                                        .group_by(self.messages_table.c.uuid)
                                        .having(sqlalchemy.func.count() > 1)
                                        .limit(bindparam("limit")),
                                        {"limit":limit}
                                        )
            return result.all()

    async def get_content_duplicates(self, limit: int = 1):
        """
        List messages which are probably duplicated based on their content checksums
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(sqlalchemy.func.max(self.messages_table.c.id),
                                                          self.messages_table.c.topic,
                                                          self.messages_table.c.timestamp,
                                                          self.messages_table.c.message_crc32,
                                                          sqlalchemy.func.count())
                                        .select_from(self.messages_table)
                                        .group_by(self.messages_table.c.topic,
                                                  self.messages_table.c.timestamp,
                                                  self.messages_table.c.message_crc32)
                                        .having(sqlalchemy.func.count() > 1)
                                        .limit(bindparam("limit")),
                                        {"limit":limit}
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
            result = await conn.execute(sqlalchemy.select(self.messages_table.c.id)
                                        .select_from(self.messages_table)
                                        .where(self.messages_table.c.uuid == bindparam("uuid")),
                                        {"uuid":uuid})
            return result.scalar()

    async def get_message_locations(self, ids):
        """
        Get the location in the store of each of a set of messages specified by
        UUID.
        
        Return: A sequence of tuples of bucket name, key where each mesage can
                be found in the data store.
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(self.messages_table.c.bucket, self.messages_table.c.key)
                                        .select_from(self.messages_table)
                                        .where(self.messages_table.c.id.in_(bindparam("ids"))),
                                        {"ids":ids})
            return result.all()

    async def get_topics_with_public_messages(self):
        async with self.engine.connect() as conn:
            result = await conn.execute(sqlalchemy.select(sqlalchemy.distinct(self.messages_table.c.topic))
                                        .where(self.messages_table.c.public == sqlalchemy.true()))
            return [r[0] for r in result.all()]

    def _generate_query_restrictions(self, q,
                                     topics_public: Optional[List[str]]=None,
                                     topics_full: Optional[List[str]]=None,
                                     start_time: Optional[int]=None,
                                     end_time: Optional[int]=None):
        if topics_public is not None or topics_full is not None:
            if topics_public is not None and len(topics_public) > 0:
                if len(topics_public) == 1:
                    pub_clause = sqlalchemy.and_(self.messages_table.c.public == sqlalchemy.true(),
                                                 self.messages_table.c.topic == topics_public[0])
                else:
                    pub_clause = sqlalchemy.and_(self.messages_table.c.public == sqlalchemy.true(),
                                                 self.messages_table.c.topic.in_(topics_public))
            else:
                pub_clause = None
            if topics_full is not None and len(topics_full) > 0:
                if len(topics_full) == 1:
                    full_clause = self.messages_table.c.topic == topics_full[0]
                else:
                    full_clause = self.messages_table.c.topic.in_(topics_full)
            else:
                full_clause = None
            if pub_clause is not None and full_clause is not None:
                q = q.where(sqlalchemy.or_(pub_clause, full_clause))
            elif pub_clause is not None:
                q = q.where(pub_clause)
            elif full_clause is not None:
                q = q.where(full_clause)
        else: # if no topics specified, select public messages across all topics
            q = q.where(self.messages_table.c.public == sqlalchemy.true())
        if start_time is not None:
            q = q.where(self.messages_table.c.timestamp >= start_time)
        if end_time is not None:
            q = q.where(self.messages_table.c.timestamp < end_time)
        return q

    async def get_message_records(self, bookmark: Optional[str]=None, page_size: int=1024,
                                  ascending: bool=True,
                                  topics_public: Optional[List[str]]=None,
                                  topics_full: Optional[List[str]]=None,
                                  start_time: Optional[int]=None,
                                  end_time: Optional[int]=None,):
        all_topics = (topics_public or []) + (topics_full or [])
        primary_order_key = self.messages_table.c.timestamp
        time_hint = None
        if len(all_topics) != 0:
            # if filtering by topics, target data may be 'clumpy' within the table, leading to
            # poor performance fetching it.
            # try to guess based on the fraction of the table the target topics
            # occupy whether an index scan is suitable or not
            tq = sqlalchemy.select(sqlalchemy.sql.func.sum(self.topic_table.c.n_messages))
            cq = tq.where(self.topic_table.c.topic.in_(all_topics))
            async with self.engine.connect() as conn:
                r = await conn.execute(sqlalchemy.union_all(cq,tq))
                # order of union results is not gauranteed, but if one value is smaller, it must
                # be the subset count, and the larger the total. 
                c, t = sorted([row[0] for row in r.all()])
            logging.debug(f"Count of messages on target topics is {c}, fraction is {c/t}")
            if c/t < 0.002:
                # This seemingly-pointless addition is to try to suppress use of the main,
                # time-ordered index when searching for messages on sparse topics.
                # The disadvantage of suppressing use of the time-ordered index is the need to
                # collect andsort potentially many candidate result rows.
                primary_order_key = primary_order_key + 0
            elif bookmark is None and start_time is None and end_time is None:
                # if attempting to find the first page (in either order) with no time restrictions,
                # scanning the time-ordered index from one end may be slow if there are many other
                # rows 'in the way'; try to add a time restriction based on metadata to skip them.
                if ascending:
                    time_hint = self.messages_table.c.timestamp >= \
                                sqlalchemy.select(sqlalchemy.sql.func.min(self.topic_table.c.e_timestamp)) \
                                .where(self.topic_table.c.topic.in_(all_topics)).scalar_subquery()
                else:
                    time_hint = self.messages_table.c.timestamp <= \
                                sqlalchemy.select(sqlalchemy.sql.func.max(self.topic_table.c.l_timestamp)) \
                                .where(self.topic_table.c.topic.in_(all_topics)).scalar_subquery()
                
        
        q = self.messages_table.select()
        if ascending:
            q = q.order_by(primary_order_key, self.messages_table.c.id)
        else:
            q = q.order_by(sqlalchemy.desc(primary_order_key),
                           sqlalchemy.desc(self.messages_table.c.id))
        q = self._generate_query_restrictions(q, topics_public, topics_full, start_time, end_time)
        if time_hint is not None:
            q = q.where(time_hint)
        async with AsyncSession(self.engine) as session:
            page = await select_page(session, q, per_page=page_size, page=bookmark)
        return ([Base_db.MessageRecord(*r) for r in page],
                page.paging.bookmark_next if page.paging.has_next else None,
                page.paging.bookmark_previous if page.paging.has_previous else None,)

    async def count_message_records(self, topics_public: Optional[List[str]]=None,
                                    topics_full: Optional[List[str]]=None,
                                    start_time: Optional[int]=None,
                                    end_time: Optional[int]=None,):
        q = sqlalchemy.select(sqlalchemy.func.count()).select_from(self.messages_table)
        q = self._generate_query_restrictions(q, topics_public, topics_full, start_time, end_time)
        async with AsyncSession(self.engine) as session:
            count = (await session.execute(q)).scalar()
        return count

    async def search_message_text(self, query,
                                  bookmark: Optional[str]=None, page_size: int=1024,
                                  ascending: bool=True,
                                  topics_public: Optional[List[str]]=None,
                                  topics_full: Optional[List[str]]=None,
                                  start_time: Optional[int]=None,
                                  end_time: Optional[int]=None,):
        j = sqlalchemy.join(self.messages_table, self.ts_table,
                            self.messages_table.c.uuid == self.ts_table.c.uuid)
        q = self.messages_table.select().select_from(j).where(self.ts_table.c.text_data.op("@@")(sqlalchemy.func.websearch_to_tsquery(query)))
        q = self._generate_query_restrictions(q, topics_public, topics_full, start_time, end_time)
        if ascending:
            q = q.order_by(self.messages_table.c.timestamp, self.messages_table.c.id)
        else:
            q = q.order_by(sqlalchemy.desc(self.messages_table.c.timestamp),
                           sqlalchemy.desc(self.messages_table.c.id))
        async with AsyncSession(self.engine) as session:
            page = await select_page(session, q, per_page=page_size, page=bookmark)
        return ([Base_db.MessageRecord(*r) for r in page],
                page.paging.bookmark_next if page.paging.has_next else None,
                page.paging.bookmark_previous if page.paging.has_previous else None,)

    async def count_text_search_results(self, query,
                                        topics_public: Optional[List[str]]=None,
                                        topics_full: Optional[List[str]]=None,
                                        start_time: Optional[int]=None,
                                        end_time: Optional[int]=None,):
        j = sqlalchemy.join(self.messages_table, self.ts_table,
                            self.messages_table.c.uuid == self.ts_table.c.uuid)
        q = sqlalchemy.select(sqlalchemy.func.count()).select_from(j).where(self.ts_table.c.text_data.op("@@")(sqlalchemy.func.websearch_to_tsquery(query)))
        q = self._generate_query_restrictions(q, topics_public, topics_full, start_time, end_time)
        async with AsyncSession(self.engine) as session:
            count = (await session.execute(q)).scalar()
        return count

    async def get_messages_not_text_indexed(self, bookmark: Optional[str]=None, page_size: int=1024,
                                            start_time: Optional[int]=None,
                                            end_time: Optional[int]=None):
        j = sqlalchemy.join(self.messages_table, self.ts_table,
                            self.messages_table.c.uuid == self.ts_table.c.uuid, isouter=True)
        q = self.messages_table.select().select_from(j).where(self.ts_table.c.uuid == None)
        if start_time is not None:
            q = q.where(self.messages_table.c.timestamp >= start_time)
        if end_time is not None:
            q = q.where(self.messages_table.c.timestamp < end_time)
        q = q.order_by(self.messages_table.c.timestamp, self.messages_table.c.id)
        async with AsyncSession(self.engine) as session:
            page = await select_page(session, q, per_page=page_size, page=bookmark)
        return ([Base_db.MessageRecord(*r) for r in page],
                page.paging.bookmark_next if page.paging.has_next else None,
                page.paging.bookmark_previous if page.paging.has_previous else None,)

    async def get_messages_not_fully_text_indexed(self, bookmark: Optional[str]=None, page_size: int=1024,
                                                  start_time: Optional[int]=None,
                                                  end_time: Optional[int]=None):
        j = sqlalchemy.join(self.messages_table, self.ts_table,
                            self.messages_table.c.uuid == self.ts_table.c.uuid)
        q = self.messages_table.select().select_from(j).where(self.ts_table.c.text_fully_indexed == False)
        if start_time is not None:
            q = q.where(self.messages_table.c.timestamp >= start_time)
        if end_time is not None:
            q = q.where(self.messages_table.c.timestamp < end_time)
        q = q.order_by(self.messages_table.c.timestamp, self.messages_table.c.id)
        async with AsyncSession(self.engine) as session:
            page = await select_page(session, q, per_page=page_size, page=bookmark)
        return ([Base_db.MessageRecord(*r) for r in page],
                page.paging.bookmark_next if page.paging.has_next else None,
                page.paging.bookmark_previous if page.paging.has_previous else None,)


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
