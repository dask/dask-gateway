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
   docker build --no-cache -t ghcr.io/dask/dask-gateway-ci-base ./base
   docker build --no-cache -t ghcr.io/dask/dask-gateway-ci-hadoop ./hadoop
   docker build --no-cache -t ghcr.io/dask/dask-gateway-ci-pbs ./pbs
   docker build --no-cache -t ghcr.io/dask/dask-gateway-ci-slurm ./slurm
   ```

4. Verify that images seem to work

   ```shell
   # hadoop: verify that the supervisord programs starts successfully
   docker run --hostname=master.example.com --rm ghcr.io/dask/dask-gateway-ci-hadoop

   # pbs: verify that logs doesn't include errors
   docker run --hostname=pbs --rm ghcr.io/dask/dask-gateway-ci-pbs

   # slurm: verify that the supervisord programs starts successfully
   docker run --hostname=slurm --rm ghcr.io/dask/dask-gateway-ci-slurm
   ```

5. Push the images:

   ```shell
   docker push ghcr.io/dask/dask-gateway-ci-base:latest
   docker push ghcr.io/dask/dask-gateway-ci-hadoop:latest
   docker push ghcr.io/dask/dask-gateway-ci-pbs:latest
   docker push ghcr.io/dask/dask-gateway-ci-slurm:latest

   # YYYY-MM-DD format
   date=$(printf '%(%Y-%m-%d)T\n' -1)
   docker tag ghcr.io/dask/dask-gateway-ci-base:latest   ghcr.io/dask/dask-gateway-ci-base:$date
   docker tag ghcr.io/dask/dask-gateway-ci-hadoop:latest ghcr.io/dask/dask-gateway-ci-hadoop:$date
   docker tag ghcr.io/dask/dask-gateway-ci-pbs:latest    ghcr.io/dask/dask-gateway-ci-pbs:$date
   docker tag ghcr.io/dask/dask-gateway-ci-slurm:latest  ghcr.io/dask/dask-gateway-ci-slurm:$date
   docker push ghcr.io/dask/dask-gateway-ci-base:$date
   docker push ghcr.io/dask/dask-gateway-ci-hadoop:$date
   docker push ghcr.io/dask/dask-gateway-ci-pbs:$date
   docker push ghcr.io/dask/dask-gateway-ci-slurm:$date
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
docker stop --timeout=0 hadoop
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
changes in your `slurm.conf`.

### Debugging the hadoop image

Debugging the a) hadoop image build, b) container startup, and c) function when
running tests against it is worth documenting separately.

__Image build__

If you get stuck during a build step, its often helpful to exec into the
previously successfully built layer and run the failing build step manually.

```shell
# Build the base image
docker build --tag ghcr.io/dask/dask-gateway-ci-base ./base

# Build the hadoop image
docker build --tag ghcr.io/dask/dask-gateway-ci-hadoop ./hadoop
```

__Container startup__

```shell
# Start a container and watch logs from supervisord that starts the various
# programs we need to configure and run successfully.
docker run --hostname master.example.com --rm ghcr.io/dask/dask-gateway-ci-hadoop
```

If something seems wrong, dig deeper.

```shell
# Start a container and inspect the container from a shell if something doesn't
# start correctly.
docker stop hadoop --timeout=0
docker run --name hadoop --hostname master.example.com --detach --rm ghcr.io/dask/dask-gateway-ci-hadoop
docker exec -it hadoop bash

# For example, a command to run within the container, useful for debugging
# Java CLASSPATH issues for skein:
skein driver start --log-level=ALL --log=/tmp/skein.log || cat /tmp/skein.log
```

__Function when running tests__

There has been a lot of struggle with file permissions. There is complexity related to:
- `/opt/hadoop/bin/container-executor` has `6050` permissions and `root:yarn` ownership, meaning a `yarn` group user can execute as `root:yarn`.
- `yarn nodemanager` started by supervisor runs as `yarn:yarn`
- The users `alice` and `bob` can become relevant when running tests, and if they belong to `yarn` and `hadoop` groups can be important.
