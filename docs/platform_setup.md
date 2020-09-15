# Platform Setup

This document describes system setup that is needed once an EC2 instance is spun up for successfully running ETL activities under [atd-data-publishing](https://github.com/cityofaustin/atd-data-publishing]) and [atd-data-deploy](https://github.com/cityofaustin/atd-data-deploy).

## One-Time Setup Procedures

First, follow AWS processes to spin up a container running CentOS 7. The container type that has been used has 1 GB of RAM, and about 8-10 GB of disk space.

### Increasing Swap Space

At earlier times, it had been found that Docker containers unexpectedly failed at random time for no explainable reason when certain ETL processes were run. The solution was to increase the amount of RAM available. The theory is that there had been a need for short periods of "peak memory" to be allocated that exceeded the RAM available on the EC2 instance. Swap space solves that problem at the expense of disk space.

The webpage at [https://www.netweaver.uk/create-swap-file-centos-7/](https://www.netweaver.uk/create-swap-file-centos-7/) is adapted here.

#### Allocating the Swap Space and Activating It
Create an 800 MB swap file:
```bash
sudo dd if=/dev/zero of=/swap count=820 bs=1MiB
```

Set permissions, format and enable:
```bash
sudo chmod 600 /swap
sudo mkswap /swap
sudo swapon /swap
```

#### Making Swap Space Persistent Upon Reboot

This involves editing the fstab file. To do this, run the following. The "mount -a" command checks the validity of the fstab file; it should return no error message.
```bash
sudo su -
echo "/swap swap swap sw 0 0" >> /etc/fstab
mount -a
exit
```

### Setting the Time Zone

The process in [Time Zone Appendix](appendix_timezone.md) describes how to set the time zone.

At this point, the VM can be rebooted: `sudo shutdown -r now`. When the VM starts back up, check to see that the time zone is correct, and swap space is active. Look to see that the correct time zone appears, and that swap space available is nonzero.

```bash
date
free -h
```

### Building the "ctrdocker/tdp" Image

The "ctrdocker/tdp" image is built using the Dockerfile in the "atd-data-publishing" project.

Get the project (after installing Git on the EC2 instance):
```bash
sudo yum install git
mkdir -p ~/git
cd ~/git
git clone https://github.com/cityofaustin/atd-data-publishing.git
cd atd-data-publishing
```

Edit the `requirements.txt` file to add a dependency that is needed by the ETL processes:

```
boto3
```

> **TODO:** Eventually we can "bake in" some of the driver functionality that's in the "atd-data-lake" project into this project and the Docker image. A good starting point is to utilize the "atd-data-publishing/config/fake_secrets.py" template to encode secret passwords directly into the image rather than having them live in the "atd-data-lake" project.

Now, build the image: `docker build --tag ctrdocker/tdp:latest .`

> Note that if the image already exists (e.g. this process was performed earlier, and it is being rerun to upgrade dependencies), the old image should be replaced with the new. If you didn't want to replace the old image, it is possible to tag it with an older version: `docker image tag ctrdocker/tdp ctrdocker/tdp:FY19`

### Staging the "atd-data-deploy" Project

Now, get the "atd-data-deploy" project onto the EC2 instance:

```bash
cd ~/git
git clone https://github.com/cityofaustin/atd-data-deploy
mkdir -p atd-data-deploy/scripts # TODO: This needs to be in the repository.
```

### Staging the "atd-data-lake" Project

Last but not least, get the "atd-data-lake" project into the same location:

```bash
cd ~/git
git clone https://github.com/cityofaustin/atd-data-lake
```

In general, "production-ready" code is located in the "master" branch, whereas preliminary code and development efforts are located in other branches.

Before this can be run, the configuration passwords need to be set in the "src/config/config_secret.py" file, which can be started off of "src/config/config_secret.py.template". Also, the dependent resources (cloud services, catalog database, etc.) need to be available.

## Recurring Procedures

This section has procedures that may be run once or multiple times.

### Configuring the ETL Process Run Times

> **TODO:** See the note at the beginning of the [Deployment Configuration](appendix_deployconf.md) document about replacing "atd-data-deploy" with Apache Airflow.

Now "atd-data-deploy" is ready to be configured per the [Deployment Configuration](appendix_deployconf.md) scripts. When a script is configured, the following may be run:

```bash
cd ~/git/atd-data-deploy
bash build.sh
bash deploy.sh # ... when it's time to have ETL processes automatically run.
```

If "atd-data-deploy" had been run in the past, then before running the "deploy.sh" script, run `sudo crontab -e` and remove old crontab entries that will be replaced; "atd-data-deploy" doesn't manage this automatically. Alternatively, you can run "deploy.sh" and then manually remove the duplicated crontab entries.

### Manually Running or Testing an ETL Process

This section desribes manually starting the "ctrdocker/tdp" Docker container for launching ETL processes, whether it be for testing or for one-off running. This is necessary because the image contains all of the library dependencies needed by the ETL processes. The most straightforward way to do this is to start the container and have it run a Bash shell:

```bash
sudo docker run -it --rm -v ~/git:/app --network=host -w /app/atd-data-lake/src ctrdocker/tdp /bin/bash
```

Inside, you can run the ETL scripts directly. Don't forget to consider using the command-line arguments that can assist in testing ETL scripts, as noted in [Testing Appendix](appendix_testing.md). Use the `exit` command to leave and shut down the container.