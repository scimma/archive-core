from contextlib import contextmanager
import logging
import os
import pytest
import requests
import shutil
import subprocess
import time
from typing import List

try:
	import docker
	have_docker = True
	import psycopg2
except ImportError:
	have_docker = False

if os.environ.get('POSTGRES_REMOTE_DB',''):
	import psycopg

@contextmanager
def temp_environ(**vars):
    """
    A simple context manager for temporarily setting environment variables

    Kwargs:
        variables to be set and their values

    Returns:
        None
    """
    original = dict(os.environ)
    os.environ.update(vars)
    try:
        yield  # no value needed
    finally:
        # restore original data
        os.environ.clear()
        os.environ.update(original)


@contextmanager
def temp_wd(path):
	"""
	A context manager which changes and then restores the working directory
	
	Args:
		path: The working directory to use temporarily
	
	Returns: None
	"""
	orig_dir = os.getcwd()
	os.chdir(path)
	try:
		yield  # no value needed
	finally:
		os.chdir(orig_dir)


def check_command_availability(command: str, args: List[str] = []):
	try:
		result = subprocess.run([command, *args], capture_output=True)
		return result.returncode == 0
	except FileNotFoundError as err:
		return False

def check_native_postgres_availability():
	return check_command_availability("pg_ctl", ["--version"])

def check_docker_availability():
	return check_command_availability("docker", ["ps"])

class PostgresServerInfo:
	def __init__(self, host: str, port: int):
		self.host = host
		self.port = port
		self.dbname = None
		self.username = ""
		self.password = ""

class PostgresServer:
	def __init__(self):
		pass
	
	def close(self):
		pass

class NativePostgresServer(PostgresServer):
	def run_control(self, *args: List[str]):
		return subprocess.run(["pg_ctl", "-D", self.datadir, *args], capture_output=True)
	
	def write_config(self, **kwargs):
		defaults = {
			"max_connections": "100",
			"shared_buffers": "128MB",
			"dynamic_shared_memory_type": "posix",
			"min_wal_size": "80MB",
			"max_wal_size": "320MB",
			"log_timezone": "UTC",
			"datestyle": "'iso, mdy'",
			"lc_messages": "'C'",
			"lc_monetary": "'C'",
			"lc_numeric": "'C'",
			"lc_time": "'C'",
			"default_text_search_config": "'pg_catalog.english'",
			"unix_socket_directories": "'"+self.datadir+"'"
		}
		defaults.update(kwargs)
		with open(self.datadir+"/postgresql.conf", 'w') as f:
			for key, value in defaults.items():
				f.write(key)
				f.write(" = ")
				f.write(value)
				f.write('\n')

	def __init__(self, datadir: str, max_start_attempts: int = 10):
		self.datadir = str(datadir)
		logging.info("Initialiing postgres in", self.datadir)
		log_path = datadir+"/logfile"
		init_result = self.run_control("init")
		if init_result.returncode != 0:
			raise RuntimeError(f"Failed to initialize postgres in {self.datadir}: \n"
			                   f"{init_result.stderr.decode('utf-8')}")
		attempts = 0
		failed = False
		port = 32768
		logging.info("Starting postgres in", self.datadir)
		while attempts < max_start_attempts:
			try:
				os.remove(log_path)
			except FileNotFoundError:
				pass
			print("Attempting to start postgres on port", port)
			self.write_config(port=str(port))
			start_result = self.run_control("-l", log_path, "start")
			if start_result.returncode != 0:
				with open(log_path, 'r') as f:
					log_data = f.read()
					if "Address already in use" in log_data:
						attempts += 1
						port += 1
						continue
				failed = True
			break
		if attempts == max_start_attempts:
			failed = True
		if failed:
			raise RuntimeError(f"Failed to start postgres in {self.datadir}: \n"
							   f"{start_result.stderr.decode('utf-8')}")
		self.info = PostgresServerInfo("localhost", port)
		
		db_result = subprocess.run(["createdb", "-h", self.info.host, "-p", str(self.info.port)], capture_output=True)
		if db_result.returncode != 0:
			self.close()
			raise RuntimeError(f"Failed to create postgres database: \n"
							   f"{db_result.stderr.decode('utf-8')}")
		self.info.dbname = "postgres"
		
		super().__init__()
	
	def close(self):
		logging.info("Stopping postgres in", self.datadir)
		stop_result = self.run_control("stop")
		super().close()

class RemotePostgresServer(PostgresServer):
	def __init__(self):
		self.info = PostgresServerInfo(
			os.environ.get('POSTGRESQL_HOST', 'localhost'),
			os.environ.get('POSTGRESQL_PORT', 5432),
		)
		self.info.dbname = "postgres"
		self.info.username = "postgres"
		max_retries = os.environ.get('POSTGRES_REMOTE_DB_RETRIES', 10)
		while max_retries > 0:
			try:
				conn = psycopg.connect(
					dbname  = self.info.dbname,
					user    = self.info.username,
					password = "",
					host     = self.info.host,
					port     = self.info.port
				)
				conn.autocommit = True
				# create a cursor
				cur = conn.cursor()
				cur.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
				rows = cur.fetchall()
				for row in rows:
					cur.execute("drop table " + row[1] + " cascade")
				cur.close()
				conn.close()
				break
			except:
				print('Waiting 1 second to retry database connection...')
				time.sleep(1)
				max_retries -= 1
				continue
		super().__init__()

	def close(self):
		super().close()

class DockerPostgresServer(PostgresServer):
	def __init__(self):
		logging.info("Starting postgres in Docker")
		self.client = docker.from_env()
		container = self.client.containers.run(
		                "postgres", detach=True, remove=True,
		                environment={
		                  "POSTGRES_HOST_AUTH_METHOD": "trust",
		                  "POSTGRES_USER": "postgres",
		                  "POSTGRES_DB": "postgres",
		                },
		                ports={
		                  "5432/tcp": None
		                })
		self.container = container
		while self.container.status == "created":
			time.sleep(0.05)
			self.container.reload()
		if self.container.status != "running":
			raise RuntimeError(f"Failed to start postgres image: \n{self.container.logs()}")
		port = int(self.container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"])
		self.info = PostgresServerInfo("localhost", port)
		self.info.dbname = "postgres"
		self.info.username = "postgres"
		# Even though the docker container is running, the database may not have fully started up,
		# so poll until it can be connected to.
		while True:
			try:
				conn = psycopg2.connect(
					dbname  = self.info.dbname,
					user    = self.info.username,
					password = "",
					host     = self.info.host,
					port     = self.info.port
				)
				conn.close()
				break
			except:
				time.sleep(0.01)
				continue
		super().__init__()
	
	def close(self):
		logging.info(f"Stopping postgres container Docker {self.container.id}")
		self.container.stop()
		super().close()

def get_postgres_db(working_directory):
	if os.environ.get('POSTGRES_REMOTE_DB',''):
		return RemotePostgresServer()
	if check_native_postgres_availability():
		return NativePostgresServer(working_directory+"/postgres_data")
	if have_docker and check_docker_availability():
		return DockerPostgresServer()
	raise RuntimeError("No available mechanism to run PostgreSQL")

@contextmanager
def temp_postgres(tmpdir):
	"""
	A context manager which creates an ephemeral PostgreSQL database
	
	Returns: A dictionary of configuration settings describing how to connect to the database
	"""
	ps = get_postgres_db(tmpdir)
	try:
		with temp_environ(DB_PASSWORD=""):
			yield {
				"db_host": ps.info.host,
				"db_port": ps.info.port,
				"db_name": ps.info.dbname,
				"db_username": ps.info.username,
			}
	finally:
		ps.close()

def check_native_minio_availability():
	return check_command_availability("minio", ["--version"])

class MinIOServerInfo:
	def __init__(self, host: str, port: int, user: str, password: str):
		self.host = host
		self.port = port
		self.user = user
		self.password = password

class MinIOServer:
	def __init__(self):
		pass
	
	def close(self):
		pass

class NativeMinIOServer(MinIOServer):
	def __init__(self, datadir: str, max_start_attempts: int = 10):
		self.datadir = datadir
		minio_bin_path = shutil.which("minio")
		if minio_bin_path is None:
			raise RuntimeError("Unable to locate minio binary")
		port = 32778
		env = {
			"MINIO_ROOT_USER": "minio",
			"MINIO_ROOT_PASSWORD": "miniopass",
			"PATH": os.environ.get("PATH",""),
			"MINIO_BROWSER": "off",
		}
		attempts = 0
		failed = False
		print("Starting minio in", self.datadir)
		while attempts < max_start_attempts:
			print("Attempting to start minio on port", port)
			args = [minio_bin_path, "server", "--address", f":{port}", datadir]
			proc = subprocess.Popen(args,
									env=env, bufsize=0, 
									stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
									)
			os.set_blocking(proc.stdout.fileno(), False)
			output = ""
			status = proc.poll()
			while status is None:
				output = proc.stdout.read()
				if output is not None:
					output = output.decode("utf-8")
					if "MinIO Object Storage Server" in output or "Error" in output:
						break
				time.sleep(0.01)
				status = proc.poll()
				
			if status is not None: # child has ended already, something is wrong
				if output is None:
					os.set_blocking(proc.stdout.fileno(), False)
					output = proc.stdout.read().decode("utf-8")
				if "Specified port is already in use" in output:
					print("Need to pick different port")
					attempts += 1
					port += 1
					continue
				failed = True
			break
		
		if attempts == max_start_attempts:
			failed = True
		if failed:
			raise RuntimeError(f"Failed to start minio in {self.datadir}: \n"
							   f"{output}")
		self.proc = proc
		self.info = MinIOServerInfo("localhost", port, "minio", "miniopass")

	def check_bucket_exists(self, bucket):
		return os.stat(f"{self.datadir}/{bucket}")
	
	def close(self):
		self.proc.send_signal(2)
		self.proc.wait()


class DockerMinIOServer(MinIOServer):
	def __init__(self):
		logging.info("Starting minio in Docker")
		self.client = docker.from_env()
		container = self.client.containers.run(
		                "quay.io/minio/minio",
		                "server /data",
		                detach=True, remove=True,
		                environment={
		                	"MINIO_BROWSER": "off",
		                },
		                ports={
		                	"9000/tcp": None
		                })
		self.container = container
		while self.container.status == "created":
			time.sleep(0.05)
			self.container.reload()
		if self.container.status != "running":
			raise RuntimeError(f"Failed to start minio image: \n{self.container.logs()}")
		port = int(self.container.attrs["NetworkSettings"]["Ports"]["9000/tcp"][0]["HostPort"])
		self.info = MinIOServerInfo("localhost", port, "minioadmin", "minioadmin")
		# Even though the docker container is running, the object store may not have fully started up,
		# so poll until it can be connected to.
		while True:
			try:
				response = requests.get(f"http://localhost:{port}/")
				if "<Error><Code>AccessDenied</Code>" in response.text:
					break
			except:
				pass
			time.sleep(0.01)
		super().__init__()
		
	def check_bucket_exists(self, bucket:str):
		result = self.container.exec_run(f"stat /data/{bucket}")
		return b"No such file or directory" not in result[1]
	
	def close(self):
		logging.info(f"Stopping minio container Docker {self.container.id}")
		self.container.stop()
		super().close()


def get_minio(working_directory):
	if check_native_minio_availability():
		return NativeMinIOServer(working_directory+"/minio_data")
	if have_docker and check_docker_availability():
		return DockerMinIOServer()
	raise RuntimeError("No available mechanism to run MinIO")

@contextmanager
def temp_minio(tmpdir):
	"""
	A context manager which creates an ephemeral MinIO object store server
	
	Returns: A dictionary of configuration settings describing how to connect to the object store
	"""
	ms = get_minio(tmpdir)
	try:
		with temp_environ(S3_ACCESS_KEY_ID=ms.info.user, S3_SECRET_ACCESS_KEY=ms.info.password):
			yield {
				"store_endpoint_url": f"http://{ms.info.host}:{ms.info.port}",
				"store_region_name": "us-east-1",
				"store_primary_bucket": "archive",
				"store_backup_bucket": "backup",
				"__test_store_server": ms,
			}
	finally:
		ms.close()
