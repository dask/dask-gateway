FROM daskgateway/testing-base:latest
MAINTAINER jcrist

# Install common utilities
RUN yum install -y epel-release \
    && yum install -y \
        sudo \
        bzip2 \
        java-1.8.0-openjdk \
        supervisor \
    && yum clean all \
    && rm -rf /var/cache/yum

# Install kerberos
RUN yum install -y \
        krb5-libs \
        krb5-server \
        krb5-workstation \
    && yum clean all \
    && rm -rf /var/cache/yum

# Install hadoop
RUN cd /etc/yum.repos.d/ && { curl -O https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/cloudera-cdh5.repo ; cd -; } \
    && rpm --import https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/RPM-GPG-KEY-cloudera \
    && yum install -y \
        hadoop-yarn-resourcemanager \
        hadoop-hdfs-namenode \
        hadoop-yarn-nodemanager \
        hadoop-hdfs-datanode \
        hadoop-client \
        hadoop-libhdfs \
    && yum clean all \
    && rm -rf /var/cache/yum

# Copy over files
COPY ./files /

# Fix container-executor permissions
RUN chmod 6050 /etc/hadoop/conf.kerberos/container-executor.cfg

# Configure and setup hadoop
RUN /root/setup-hadoop.sh

# Setup kerberos
RUN /root/setup-kerb.sh

ENV JAVA_HOME /usr/lib/jvm/jre-openjdk
ENV LIBHDFS3_CONF /etc/hadoop/conf/hdfs-site.xml
ENV HADOOP_CONF_DIR /etc/hadoop/conf
ENV HADOOP_HOME /usr/lib/hadoop
ENV HADOOP_COMMON_HOME /usr/lib/hadoop
ENV HADOOP_YARN_HOME /usr/lib/hadoop-yarn
ENV HADOOP_HDFS_HOME /usr/lib/hadoop-hdfs
ENV TEST_DASK_GATEWAY_YARN true

CMD ["/usr/bin/supervisord", "--configuration", "/etc/supervisord.conf"]
