# archive-core

This library implements the core functionality of the Hopskotch Archive. 
It is intended to be the sole (internal) interface for storage and retrieval. 

## Installation

Installation should work normally with `pip`:

	pip install 'archive-core @ git+https://github.com/scimma/archive-core'

Or, for development in a checked out copy of this repository:

	pip install .

## Tests

The library has a full suite of tests which work with `pytest`. 
To install the tests' python dependencies, run:

	pip install -r tests/requirements.txt

A number of the test require either a PostgreSQL or MinIO server, which can be run automatically if
these tools are available in the environment, or otherwise can be run via Docker if that is
installed. Using native binaries is by far the fastest option, while using Docker enables installing
fewer tools on the system. 

The provided makefile includes a target for running the tests, so

	make test

is the easiest way to run the full test suite.

### Socket Issues for Native PostgreSQL on Mac OS

On Mac OS, it can be the case that the combination of the long default temporary directory names
with pytest's deeply nested subdirectories creates paths longer than PostgreSQL can tolerate for its
socket. This manifests as test failing with errors like:

	FAILED tests/test_database_api.py::test_SQL_db_startup - RuntimeError: Failed to start postgres in /private/var/folders/b9/37g2pd1d2qj33_09tf646ljh0000gn/T/pytest-of-user/pytest-14/test_SQL_db_startup0/postgres_data:

and examining the resulting postgres logfile will show:

	Unix-domain socket path "/private/var/folders/b9/37g2pd1d2qj33_09tf646ljh0000gn/T/pytest-of-user/pytest-13/test_SQL_db_startup0/postgres_data/.s.PGSQL.32770" is too long (maximum 103 bytes)

This can be mitigated by creating a temporary directory with a relatively short path
(e.g. `/tmp/test`) and instructing pytest to use it:

	make test PYTEST_ARGS='--basetemp=/tmp/test'
	
One can also `export` the setting of `PYTEST_ARGS` in the shell so that it is picked up by plain
`make test`. 

The `PYTEST_ARGS` variable can also be useful for running selected tests during debugging, e.g.:

	make test PYTEST_ARGS='-k test_DbFactory'

will run only `test_DbFactory`. 

### Run tests in a Docker Compose environment

This method is not recommended, as it is slower than running the tests natively with their
dependencies run via Docker, while adding an additional dependency on `docker-compose`. 
Additionally, it provides no simple way to run individual tests

After cloning this repo, execute the following command to build the container and run the tests:

```bash
docker compose up --build
```
