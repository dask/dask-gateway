FROM centos:centos7
MAINTAINER jcrist

# Install common utilities
RUN yum install -y \
        sudo \
        bzip2 \
        java-1.8.0-openjdk \
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

# Install supervisord
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python get-pip.py \
    && pip install supervisor \
    && rm get-pip.py

# Make a few user accounts
RUN useradd -m dask
RUN useradd -m alice
RUN useradd -m bob

# Install miniconda
RUN curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -p /opt/miniconda \
    && rm /tmp/miniconda.sh \
    && /opt/miniconda/bin/conda update conda -y \
    && /opt/miniconda/bin/conda config --set always_yes yes --set changeps1 no \
    && /opt/miniconda/bin/conda clean -a \
    && find /opt/miniconda/ -type f -name '*.a' -delete \
    && find /opt/miniconda/ -type f -name '*.js.map' -delete \
    && find /opt/miniconda/ -type f -name '*.pyc' -delete \
    && rm -rf /opt/miniconda/pkgs \
    && find /opt/miniconda/ -type f -name '*.so' -exec strip --strip-all {} + || true

# Install go
RUN curl https://dl.google.com/go/go1.12.2.linux-amd64.tar.gz -o /tmp/go.tar.gz \
    && tar -xzf /tmp/go.tar.gz -C /opt/ \
    && rm /tmp/go.tar.gz

RUN echo 'export PATH="/opt/go/bin:/opt/miniconda/bin:$PATH"' >> ~/.bashrc

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

CMD ["supervisord", "--configuration", "/etc/supervisord.conf"]
