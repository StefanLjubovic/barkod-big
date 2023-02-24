# Flight server application

Flight server application respresents module for ingestion, transformation and visualization of flight data and flight key performance indicators.
Flight and KPI data is acquired through Kafka topics as a Avro encoded messages (events).

## Getting Started

Prerequisites:

- Python 3.9
- Poetry
- docker-compose

Poetry installation steps are on the following [link](https://python-poetry.org/docs/#installation).

### Installation

After poetry installation, create virtual environment with Python 3.9.
`venv` module, or some of the other virtual environment
packages such as `virtualenv`.

Activate the virtual environment and run

```bash
$ poetry install
```

The existing `poetry.lock` file is used to pin required dependency versions.

### Server

Server (consumer) can be started with

```bash
$ python -m server.main
```

This will start worker configured with [default settings](./server/settings.toml).

Server can be started with docker-compose as well, located in the infrastructure folder.
