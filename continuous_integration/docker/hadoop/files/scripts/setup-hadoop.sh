#!/bin/bash
set -ex

# Tweak hadoop configuration and permissions:
#
# - hadoop is unpacked with default configuration in etc/hadoop, we relocate
#   that to /etc/hadoop/conf.empty.
#
mv /opt/hadoop/etc/hadoop /etc/hadoop/conf.empty
#
# - log4j.properties is a requirement to have in the hadoop configuration
#   directory that we don't wan't to redefine, so we copy it from the default
#   configuration to our configurations.
#
cp /etc/hadoop/conf.empty/log4j.properties /etc/hadoop/conf.simple/
cp /etc/hadoop/conf.empty/log4j.properties /etc/hadoop/conf.kerberos/
#
# - Create /opt/hadoop/logs directory with high group permissions to ensure it
#   isn't created with narrow permissions later when running "hdfs namenode".
#
mkdir -p /opt/hadoop/logs
chmod g+w /opt/hadoop/logs
#
# - Create /var/tmp directory with permissions to ensure the hadoop group is
#   propegated and have right to create new directories. Note that the hdfs user
#   will later create /var/tmp/dfs but then get to own it even though it will be
#   owned also by the hadoop group due to the 2xxx part of these permissions.
#
chown -R root:hadoop /var/tmp
chmod -R 2770 /var/tmp
#
# - Generate a key to authenticate web access during the brief time we use the
#   /etc/hadoop/conf.simple configuration as part of building the docker image.
#
dd if=/dev/urandom bs=64 count=1 > /etc/hadoop/conf.simple/http-secret-file
chown root:hadoop /etc/hadoop/conf.simple/http-secret-file
chmod 440 /etc/hadoop/conf.simple/http-secret-file
#
# - Declare HDFS configuration to use temporarily, let /opt/hadoop/etc/hadoop
#   point to /etc/hadoop/conf.simple.
#
alternatives --install /opt/hadoop/etc/hadoop hadoop-conf /etc/hadoop/conf.simple 50
alternatives --set hadoop-conf /etc/hadoop/conf.simple




# Initialize HDFS filesystem with content to test against
#
# 1. Delete all hdfs files and start with a clean slate.
#
sudo --preserve-env --user hdfs \
    hdfs namenode -format -force
#
# 2. Add to hosts to resolve a domain name, /etc/hosts will be cleared when the
#    container starts though, see https://stackoverflow.com/a/25613983. This
#    container is supposed to start with "--hostname master.example.com".
#
echo "127.0.0.1 master.example.com" >> /etc/hosts
#
# 3. Start "hdfs namenode" and "hdfs datanode" but detach with "&" to continue
#    doing other things.
#
sudo --preserve-env --user hdfs \
    hdfs namenode &
sudo --preserve-env --user hdfs \
    hdfs datanode &
#
# 4. Run a script to bootstrap the HDFS filesystem with content for testing.
#
sudo --preserve-env --user hdfs \
    /scripts/init-hdfs.sh
#
# 5. Shut down started "hdfs namenode" and "hdfs datanode" processes.
#
pkill java
