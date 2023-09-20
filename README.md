# dagsterpipe

This is a pipeline project using [Dagster](https://dagster.io/).

## Getting started

First, create a virtual environment and activate it:

```bash
python3 -m venv dagsterpipe-venv
source dagster-venv/bin/activate
```

Then, install the project dependencies using poetry:

```bash
pip install poetry
poetry install
```


Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.


### Unit testing

Tests are in the `dagsterpipe_tests` directory and you can run tests using `pytest`:

```bash
pytest dagsterpipe_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.
