FROM python:3.11

RUN useradd --create-home --shell /bin/bash worker --uid 1000

RUN wget https://dl.min.io/server/minio/release/linux-amd64/archive/minio_20230831153116.0.0_amd64.deb -O minio.deb && dpkg -i minio.deb && rm minio.deb

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
  postgresql-15         \
  postgresql-common  \
  && rm -rf /var/lib/apt/lists/*

ENV PATH=/home/worker/.local/bin:/usr/lib/postgresql/15/bin/:$PATH
RUN chown -R 102:1000 /var/run/postgresql

USER worker

RUN mkdir /home/worker/src
WORKDIR /home/worker/src

COPY --chown=worker:worker ./docker ./docker
RUN pip install -r docker/requirements.txt
COPY --chown=worker:worker ./pyproject.toml .
RUN python docker/install_deps.py
RUN cat /tmp/requirements.txt && pip install -r /tmp/requirements.txt

COPY --chown=worker:worker ./tests/requirements.txt /tmp/requirements.test.txt
RUN pip install -r /tmp/requirements.test.txt

COPY --chown=worker:worker ./ ./

RUN pip install .
