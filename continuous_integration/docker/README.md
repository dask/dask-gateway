# About these Dockerfiles

As dask-gateway can be used to start different kinds of dask clusters, we need
to be able to test against those dask cluster backends. To do that we maintain
docker images setup to run the various dask cluster backends so we can test
against them.

The images doesn't install `dask-gateway-server` within them as then we would
need to rebuild the images all the time with the specific version of
`dask-gateway-server` we want to test. Instead, the idea is to mount the local
code to a container and install dependencies before that before running the
tests. For example the `start.sh` script starts a container, and
`install.sh`/`test.sh` are wrappers to run `_install.sh`/`_test.py` scripts
in the started container.

## Manual build and update of images

For now these images are built and updated manually. Below are instructions for
a maintainer of the dask/dask-gateway repo on how to do it.

1. Create a personal access token (PAT) for your account with `write:packages`
   permissions at https://github.com/settings/tokens/new.

2. Login to the ghcr.io container registry with the PAT:

   ```shell
   docker login ghcr.io -u your-username
   ```

3. Build the images:

   ```shell
   docker build --no-cache -t ghcr.io/meta-introspector/dask-gateway-ci-base ./base
   docker build --no-cache -t ghcr.io/meta-introspector/dask-gateway-ci-hadoop ./hadoop
   docker build --no-cache -t ghcr.io/meta-introspector/dask-gateway-ci-pbs ./pbs
   docker build --no-cache -t ghcr.io/meta-introspector/dask-gateway-ci-slurm ./slurm
   ```

4. Verify that images seem to work

   ```shell
   # hadoop: verify that the supervisord programs starts successfully:
   #
   #   ...
   #   kadmind entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
   #   krb5kdc entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
   #   hdfs-namenode entered RUNNING state, process has stayed up for > than 3 seconds (startsecs)
   #   hdfs-datanode entered RUNNING state, process has stayed up for > than 3 seconds (startsecs)
   #   yarn-resourcemanager entered RUNNING state, process has stayed up for > than 3 seconds (startsecs)
   #   yarn-nodemanager entered RUNNING state, process has stayed up for > than 3 seconds (startsecs)
   #
   docker run --hostname=master.example.com --rm ghcr.io/meta-introspector/dask-gateway-ci-hadoop

   # pbs: verify that logs doesn't include errors
   #
   #   ...
   #   PBS server
   #   + /opt/pbs/bin/qmgr -c 'set server scheduler_iteration = 20'
   #   + /opt/pbs/bin/qmgr -c 'set server job_history_enable = True'
   #   + /opt/pbs/bin/qmgr -c 'set server job_history_duration = 24:00:00'
   #   + /opt/pbs/bin/qmgr -c 'set node pbs queue=workq'
   #   + /opt/pbs/bin/qmgr -c 'set server operators += dask@pbs'
   #   Entering sleep
   #   + echo 'Entering sleep'
   #   + sleep infinity
   #
   docker run --hostname=pbs --rm ghcr.io/meta-introspector/dask-gateway-ci-pbs

   # slurm: verify that the supervisord programs starts successfully
   #
   #   ...
   #   mysqld entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
   #   slurmdbd entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
   #   slurmd entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
   #   slurmctld entered RUNNING state, process has stayed up for > than 3 seconds (startsecs)
   #   munged entered RUNNING state, process has stayed up for > than 5 seconds (startsecs)
   #
   docker run --hostname=slurm --rm ghcr.io/meta-introspector/dask-gateway-ci-slurm
   ```

5. Push the images:

   ```shell
   docker push ghcr.io/meta-introspector/dask-gateway-ci-base:latest
   docker push ghcr.io/meta-introspector/dask-gateway-ci-hadoop:latest
   docker push ghcr.io/meta-introspector/dask-gateway-ci-pbs:latest
   docker push ghcr.io/meta-introspector/dask-gateway-ci-slurm:latest

   # YYYY-MM-DD format
   date=$(date '+%Y-%m-%d')
   docker tag ghcr.io/meta-introspector/dask-gateway-ci-base:latest   ghcr.io/meta-introspector/dask-gateway-ci-base:$date
   docker tag ghcr.io/meta-introspector/dask-gateway-ci-hadoop:latest ghcr.io/meta-introspector/dask-gateway-ci-hadoop:$date
   docker tag ghcr.io/meta-introspector/dask-gateway-ci-pbs:latest    ghcr.io/meta-introspector/dask-gateway-ci-pbs:$date
   docker tag ghcr.io/meta-introspector/dask-gateway-ci-slurm:latest  ghcr.io/meta-introspector/dask-gateway-ci-slurm:$date
   docker push ghcr.io/meta-introspector/dask-gateway-ci-base:$date
   docker push ghcr.io/meta-introspector/dask-gateway-ci-hadoop:$date
   docker push ghcr.io/meta-introspector/dask-gateway-ci-pbs:$date
   docker push ghcr.io/meta-introspector/dask-gateway-ci-slurm:$date
   ```

## Running tests

Below is an example on how you would run tests against the hadoop image, which
is now assumed to have been built. You would do the same for the slurm and pbs
image.

```shell
cd ./hadoop

# starts a docker container, mounting files from this local git repo
./start.sh

# installs requirements within the container
./install.sh

# runs tests within the container
./test.sh

# print relevant logs from the container for debugging
./print_logs.sh

# cleanup after running tests
docker stop --time=0 hadoop
```

## Debugging

### General advice

1. If you get a `docker build` error, you can do `docker run -it --rm <hash>` to
   a saved layer before the erroring step and then manually do the next `RUN`
   step or inspect the file system of its current state. Note that intermediary
   layers are not saved if you have set `export DOCKER_BUILDKIT=1`, so this
   trick can only be used without buildkit.
1. A Dockerfile's `COPY` command can update permissions of folders if you let it
   copy nested folders. For example, `COPY ./files /` would update the
   permissions of `/etc` based on the permissions set on the folder and files in
   this git repo locally.
1. File permissions you have set in this git repo locally won't be version
   controlled, besides the execute bit. Due to that, you must avoid relying on
   local file permissions when building images.

### Debugging the slurm image

If you upgrade `slurm` to a new version, you may very well run into breaking
changes in your `slurm.conf`. See the [slurm release notes][] for an overview of
changes. If there is a systemd service that fails to startup, you could try
inspect its log file that is declared in `slurm.conf` found in this repo.

```shell
docker ps
docker exec -it <container name> bash

# print logs from within the running slurm container
cat /var/log/slurm/slurmd.log
cat /var/log/slurm/slurmctld.log
```

[slurm release notes]: https://slurm.schedmd.com/news.html

### Debugging the hadoop image

Debugging the a) hadoop image build, b) container startup, and c) function when
running tests against it is worth documenting separately.

__Image build__

If you get stuck during a build step, its often helpful to exec into the
previously successfully built layer and run the failing build step manually.

```shell
# Build the base image
docker build --tag ghcr.io/meta-introspector/dask-gateway-ci-base ./base

# Build the hadoop image
docker build --tag ghcr.io/meta-introspector/dask-gateway-ci-hadoop ./hadoop
```

__Container startup__

```shell
# Start a container and watch logs from supervisord that starts the various
# programs we need to configure and run successfully.
docker run --hostname master.example.com --rm ghcr.io/meta-introspector/dask-gateway-ci-hadoop
```

If something seems wrong, dig deeper.

```shell
# Start a container and inspect the container from a shell if something doesn't
# start correctly.
docker stop --time=0 hadoop
docker run --name hadoop --hostname master.example.com --detach --rm ghcr.io/meta-introspector/dask-gateway-ci-hadoop
docker exec -it hadoop bash

# For example, a command to run within the container, useful for debugging
# Java CLASSPATH issues for skein:
skein driver start --log-level=ALL --log=/tmp/skein.log || cat /tmp/skein.log

# A systemd service may have failed to startup, with registered logfile to inspect
cat /var/log/supervisor/yarn-nodemanager.log
cat /var/log/supervisor/yarn-resourcemanager.log
cat /var/log/supervisor/hdfs-datanode.log
cat /var/log/supervisor/hdfs-namenode.log
cat var/log/supervisor/krb5libs.log
cat var/log/supervisor/krb5kdc.log
cat var/log/supervisor/kadmind.log
```

__Function when running tests__

There has been a lot of struggle with file permissions. There is complexity related to:
- `/opt/hadoop/bin/container-executor` has `6050` permissions and `root:yarn` ownership, meaning a `yarn` group user can execute as `root:yarn`.
- `yarn nodemanager` started by supervisor runs as `yarn:yarn`
- The users `alice` and `bob` can become relevant when running tests, and if they belong to `yarn` and `hadoop` groups can be important.
