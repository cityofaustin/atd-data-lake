# Appendix: Deployment Configuration

*[(Back to Docs Catalog)](index.md)*

Deployment of the ETL scripts is coordinated through the [atd-data-deploy](https://github.com/cityofaustin/atd-data-deploy) project. The script runs from within a Docker container built with the [atd-data-publishing](https://github.com/cityofaustin/atd-data-publishing]) project (with added dependencies as noted below), and in these examples retrieves transformation code from directories mounted on the host system. This document describes the configurations that are currently running on the two servers that run ETL tasks.

> Note that Apache Airflow has been evaluated as a replacement for the "atd-data-deploy" scheme. While there's still a good case for using Docker containers, the "atd-data-deploy" configurations would be replaced with a script that coordinates Airflow, with improved handling of error conditions, sequences of events, and logging.

## Script Server: Inside the ATD Network

The Script Server is a system that has network access to sensor devices deployed on the ATD city-wide network, as well as resources running within the ATD organization.

First, the `docker.yml` configuration file for "transportation-data-deploy" is set with a couple of configurations. Note that the `ctr-awam` configuration allows for connection to a mounted "AWAM" network share that contains files produced by the Post Oak Bluetooth readers.

```yml
ctr:
  args:
  - $CMD
  command: python
  image: ctrdocker/tdp
  options:
  - -d
  - -v $BUILD_PATH:/app
  - --rm
  - --network=host
  - -w /app/$WORKDIR
ctr-awam:
  args:
  - $CMD
  command: python
  image: ctrdocker/tdp
  options:
  - -d
  - -v $BUILD_PATH:/app
  - -v /mnt/awam:/app/awam
  - --rm
  - --network=host
  - -w /app/$WORKDIR
```

Then, the individual scripts that run on the Script Server are configured in `scripts.yml`:

```yml
bt_insert_lake:
  cron: 30 0 * * *
  destination: raw
  docker_cmd: ctr-awam
  enabled: true
  filename: bt_insert_lake.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
  - --sourcedir
  - "/app/awam"
wt_insert_lake:
  cron: 0 1 * * *
  destination: raw
  docker_cmd: ctr
  enabled: true
  filename: wt_insert_lake.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
gs_insert_lake:
  cron: 30 1 * * *
  destination: raw
  docker_cmd: ctr
  enabled: true
  filename: gs_insert_lake.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
```

## EC2 Instance: Outside the ATD Network

The EC2 instance runs transformation activities that don't depend upon access to items that are only reachable from within the ATD network. A benefit of running these scripts from the EC2 instance lies in how accesses to S3 are charged: since data for these transformations don't leave the AWS domain, charges for offsite data transfers are not incurred.

For information on configuring the EC2 instance for running these ETL jobs, please refer to the [Platform Setup](platform_setup.md) document.

This portion is the `docker.yml` configuration:

```yml
ctr:
  args:
  - $CMD
  command: python
  image: ctrdocker/tdp
  options:
  - -d
  - -v $BUILD_PATH:/app
  - --rm
  - --network=host
  - -w /app/$WORKDIR
```

Then, this `scripts.yml` defines all of the transformations that are currently running.

```yml
bt_json_standard:
  cron: 0 3 * * *
  destination: rawjson
  docker_cmd: ctr
  enabled: true
  filename: bt_json_standard.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
bt_ready:
  cron: 0 5 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: bt_ready.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
wt_json_standard:
  cron: 30 6 * * *
  destination: rawjson
  docker_cmd: ctr
  enabled: true
  filename: wt_json_standard.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
wt_ready:
  cron: 30 6 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: wt_ready.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
wt_soc:
  cron: 30 7 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: wt_extract_soc.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
gs_json_standard:
  cron: 0 4 * * *
  destination: rawjson
  docker_cmd: ctr
  enabled: true
  filename: gs_json_standard.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
gs_ready:
  cron: 30 5 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: gs_ready.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
gs_ready_agg:
  cron: 0 6 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: gs_ready_agg.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
bt_soc:
  cron: 0 7 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: bt_extract_soc.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
gs_ready_agg_soc:
  cron: 30 8 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: gs_agg_extract_soc.py
  init_func: main
  job: true
  path: ../atd-data-lake/src
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --start_date
  - "60"
```

### Preparing the Jobs

