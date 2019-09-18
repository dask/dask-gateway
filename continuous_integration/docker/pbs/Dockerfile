FROM daskgateway/testing-base:latest

# Install pbspro
RUN yum install -y unzip \
    && curl -L -o /tmp/pbspro.zip https://github.com/PBSPro/pbspro/releases/download/v18.1.4/pbspro_1.8.4.centos7.zip \
    && unzip /tmp/pbspro.zip -d /tmp/pbspro \
    && yum install -y sudo /tmp/pbspro/pbspro*/pbspro-server-*.rpm \
    && yum remove -y unzip \
    && yum clean all \
    && rm -rf /var/cache/yum

# Notify dask-gateway tests that PBS is available
ENV TEST_DASK_GATEWAY_PBS true
ENV PBS_MASTER pbs_master

# Copy over files
COPY ./files /

ENTRYPOINT ["/opt/miniconda/bin/tini", "-g", "--"]
CMD ["/root/start.sh"]
