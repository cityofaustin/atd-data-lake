# Appendix: Deployment Configuration

*[(Back to Docs Catalog)](index.md)*

Deployment of the ETL scripts is coordinated through the [cityofaustin/atd-data-deploy](https://github.com/cityofaustin/atd-data-deploy) "transportation-data-deploy" script. The script runs from within a Docker container built with the [transportation-data-publishing](https://github.com/cityofaustin/atd-data-publishing]) project, and in these examples retrieves transformation code from directories mounted on the host system. This document describes the configurations that are currently running on the two servers that run ETL tasks.

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
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --months_old
  - "1"
  - --source_dir
  - "/app/awam"
  - --missing
wt_insert_lake:
  cron: 0 1 * * *
  destination: raw
  docker_cmd: ctr
  enabled: true
  filename: wt_insert_lake.py
  init_func: main
  job: true
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --months_old
  - "1"
gs_insert_lake:
  cron: 30 1 * * *
  destination: raw
  docker_cmd: ctr
  enabled: true
  filename: gs_insert_lake.py
  init_func: main
  job: true
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --months_old
  - "1"
```

## EC2 Instance: Outside the ATD Network

The EC2 instance runs transformation activities that don't depend upon access to items that are only reachable from within the ATD network. A benefit of running these scripts from the EC2 instance lies in how accesses to S3 are charged: since data for these transformations don't leave the AWS domain, charges for offsite data transfers are not incurred.

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
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --months_old
  - "1"
bt_ready:
  cron: 0 5 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: bt_ready.py
  init_func: main
  job: true
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --months_old
  - "1"
wt_json_standard:
  cron: 30 3 * * *
  destination: rawjson
  docker_cmd: ctr
  enabled: true
  filename: wt_json_standard.py
  init_func: main
  job: true
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
gs_json_standard:
  cron: 0 4 * * *
  destination: rawjson
  docker_cmd: ctr
  enabled: true
  filename: gs_json_standard.py
  init_func: main
  job: true
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --months_old
  - "1"
  - --last_run_date
  - "0"
gs_ready:
  cron: 30 5 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: gs_ready.py
  init_func: main
  job: true
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --months_old
  - "1"
gs_ready_agg:
  cron: 0 6 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: gs_ready_agg.py
  init_func: main
  job: true
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
bt_soc:
  cron: 0 7 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: bt_extract_soc.py
  init_func: main
  job: true
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --months_old
  - "1"
gs_ready_agg_soc:
  cron: 30 8 * * *
  destination: ready
  docker_cmd: ctr
  enabled: true
  filename: gs_agg_extract_soc.py
  init_func: main
  job: true
  path: ../coa_dev/aws_transport
  source: atd-data-lake
  args:
  - --last_run_date
  - "0"
  - --months_old
  - "1"
```
