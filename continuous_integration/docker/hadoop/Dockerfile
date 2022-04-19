# See continuous_integration/docker/README.md for details about this and other
# Dockerfiles under the continuous_integration/docker folder on their purpose
# and how to work with them.
#
FROM ghcr.io/dask/dask-gateway-ci-base:latest

# Set labels based on the Open Containers Initiative (OCI):
# https://github.com/opencontainers/image-spec/blob/main/annotations.md#pre-defined-annotation-keys
#
LABEL org.opencontainers.image.url="https://github.com/dask/dask-gateway/blob/HEAD/continuous_integration/docker/hadoop/Dockerfile"

# Notify dask-gateway tests that Yarn (part of Hadoop) is available
ENV TEST_DASK_GATEWAY_YARN true



# Install hadoop
#
# 1. Create hadoop users and groups.
#
RUN groupadd --system hadoop \
 && useradd yarn --system --no-create-home --groups=hadoop \
 && useradd hdfs --system --no-create-home --groups=hadoop \
 && useradd mapred --system --no-create-home --groups=hadoop
#
# 2. Install hadoop v3 dependencies
#
# - Java 8+ (java-1.8.0-openjdk, java-11-openjdk)
#
# - OpenSSL 1.1 (openssl 1.0 comes with centos:7, found via epel-release repo)
#
RUN yum install -y \
        epel-release \
 && yum install -y \
        java-1.8.0-openjdk \
        openssl11-libs \
 && yum clean all \
 && rm -rf /var/cache/yum
ENV JAVA_HOME /usr/lib/jvm/jre-openjdk
#
# 3. Download and unpack hadoop
#
#    hadoop versions:  https://dlcdn.apache.org/hadoop/common/
#    hadoop changelog: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/release/
#
#    We set the owner user:group to root:hadoop and declare the setuid and
#    setgid bits for this directory so that folders created in it become owned
#    by root:hadoop as well.
#
RUN INSTALL_HADOOP_VERSION=3.3.2 \
 && curl -sL /tmp/hadoop.tar.gz https://dlcdn.apache.org/hadoop/common/stable/hadoop-${INSTALL_HADOOP_VERSION}.tar.gz \
  | tar -xvz --directory /opt \
 && mv /opt/hadoop-* /opt/hadoop \
 && chown -R root:hadoop /opt/hadoop \
 && chmod ug+s /opt/hadoop
#
# 4. Configure HADOOP_ environment variables
#
#   - HADOOP_CONF_DIR, HADOOP_COMMON_HOME, HADOOP_HDFS_HOME, and
#     HADOOP_YARN_HOME are referenced by `skein` who loads a default hadoop
#     classpath refenrecing these variables in the same way we have declared
#     them in yarn-site.yaml container-executor.cfg.
#
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_COMMON_HOME=/opt/hadoop \
    HADOOP_HDFS_HOME=/opt/hadoop \
    HADOOP_YARN_HOME=/opt/hadoop
#
# 5. Update PATH environment variable
#
#    Note that this PATH environment will be preserved when sudo is used to
#    switch to other users thanks to changes to /etc/sudoers.d/preserve_path,
#    which is configured in the base Dockerfile.
#
ENV PATH=/opt/hadoop/sbin:/opt/hadoop/bin:$PATH
#
# 6. Copy our hadoop configurations
#
#    The permissions are important for function! As we copy additional files to
#    these folders later and want them to have root:hadoop ownership for
#    readability, we "chmod ug+s" as well on the folder.
#
COPY --chown=root:hadoop ./files/etc/hadoop /etc/hadoop/
RUN chmod -R ug+s /etc/hadoop/
#
# 7. Copy our setup script and run it
#
COPY ./files/scripts/setup-hadoop.sh /scripts/
COPY ./files/scripts/init-hdfs.sh /scripts/
RUN /scripts/setup-hadoop.sh



# Install kerberos
#
# 1. Install yum packages
#
RUN yum install -y \
        krb5-libs \
        krb5-server \
        krb5-workstation \
 && yum clean all \
 && rm -rf /var/cache/yum
#
# 2. Copy our kerberos configuration
#
COPY ./files/etc/krb5.conf /etc/
COPY ./files/var/kerberos/krb5kdc /var/kerberos/krb5kdc/
#
# 3. Copy our setup script and run it
#
COPY ./files/scripts/setup-kerb.sh /scripts/
RUN /scripts/setup-kerb.sh



# Install supervisor
#
# - supervisord will be the entrypoint of the container, configured to start
#   multiple services via provided files.
# - /etc/supervisor.conf declares that /etc/supervisor.d/* should be included
#   among other things.
# - /etc/supervisor.d/* declares a few supervisor programs, running the
#   following commands as specified user:
#
#   COMMAND              | USER | LOGFILE
#   -------------------- | ---- | -----------------------------------------------------
#   hdfs datanode        | hdfs | /var/log/supervisor/hdfs-datanode.log
#   hdfs namenode        | hdfs | /var/log/supervisor/hdfs-namenode.log
#   krb5kdc              | root | /var/log/supervisor/krb5kdc.log
#   kadmind              | root | /var/log/supervisor/kadmind.log
#   yarn nodemanager     | yarn | /var/log/supervisor/yarn-nodemanager.log
#   yarn resourcemanager | yarn | /var/log/supervisor/yarn-resourcemanager.log
#
# 1. Install supervisor (which requires already installed epel-release).
#
RUN yum install -y \
        supervisor \
 && yum clean all \
 && rm -rf /var/cache/yum
#
# 2. Initialize logfiles as required
#
RUN touch \
        /var/log/supervisor/kadmind.log \
        /var/log/supervisor/krb5kdc.log \
        /var/log/supervisor/krb5libs.log
#
# 3. Copy files used by supervisor
#
COPY ./files/etc/supervisord.d /etc/supervisord.d/
COPY ./files/etc/supervisord.conf /etc/
#
# 4. Configure the container to start supervisord with our configuration.
#
ENTRYPOINT ["/usr/bin/supervisord", "--configuration", "/etc/supervisord.conf"]
