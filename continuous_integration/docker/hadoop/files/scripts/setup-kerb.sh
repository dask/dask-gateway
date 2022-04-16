#!bin/bash
set -ex

# This scripts configures file system permissions and initializes kerberos with
# principals and passwords for later tests using kdb5_util and kadmin.local.
#
# References:
#   - kadmin.local: https://web.mit.edu/kerberos/krb5-1.12/doc/admin/admin_commands/kadmin_local.html?highlight=kadmin#options
#   - kdb5_util:    https://web.mit.edu/kerberos/krb5-1.12/doc/admin/admin_commands/kdb5_util.html
#

# Copy a key generated in setup-hadoop.sh to avoid a warning. Note that this
# auth may be relevant when running a yarn backend test rather then a kerberos
# auth test.
#
cp /etc/hadoop/conf.simple/http-secret-file /etc/hadoop/conf.kerberos/http-secret-file

# Tweak file system permissions and switch to new hadoop config at
# /etc/hadoop/conf.kerberos.
#
# - The /opt/hadoop/bin/container-executor binary has stringent permissions
#   requirements on itself and its configuration.
#
#   About binary:
#     - user-owned by root / group-owned by special group (chown root:yarn,
#       assuming yarn is the configured container-executor group in
#       yarn-site.xml and container-executor.cfg).
#     - others do not have any permissions (chmod xxx0)
#     - be setuid/setgid (chmod 6xxx)
#     - Reference: https://github.com/apache/hadoop/blob/907ef6c2858dc42d83cc228d13409830a7d7b163/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/native/container-executor/impl/container-executor.h#L78-L88
#
#   About config:
#     - user-owned by root
#     - not writable by group / other
#     - Reference: https://github.com/apache/hadoop/blob/03cfc852791c14fad39db4e5b14104a276c08e59/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/native/container-executor/impl/configuration.c#L78-L100
#
chown root:yarn /opt/hadoop/bin/container-executor
chmod 6050 /opt/hadoop/bin/container-executor
chmod 440 /etc/hadoop/conf.kerberos/container-executor.cfg
#
# - Create directories declared both in /etc/hadoop/conf.kerberos/yarn-site.xml
#   and /etc/hadoop/conf.kerberos/container-executor.cfg about:
#
#     - yarn.nodemanager.local-dirs
#     - yarn.nodemanager.log-dirs
#
#   Weird reference, not to be trusted about permissions for hadoop v3 it seems:
#   https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/SecureContainer.html#Configuration
#
#   Note that folders created in /var/tmp/hadoop-yarn/local will have reset
#   owners and permissions when yarn nodemanager starts so we can't try to
#   influence it from here.
#
mkdir -p \
    /var/tmp/hadoop-yarn/log \
    /var/tmp/hadoop-yarn/local
chown -R yarn:yarn /var/tmp/hadoop-yarn
chmod -R 770 /var/tmp/hadoop-yarn
usermod -G yarn -a alice
usermod -G yarn -a bob
usermod -G hadoop -a alice
usermod -G hadoop -a bob
#
# - Declare HDFS configuration to use, let /opt/hadoop/etc/hadoop point to
#   /etc/hadoop/conf.kerberos now instead of /etc/hadoop/conf.simple that was
#   used to bootstrap hdfs with folders and files.
#
alternatives --install /opt/hadoop/etc/hadoop hadoop-conf /etc/hadoop/conf.kerberos 50
alternatives --set hadoop-conf /etc/hadoop/conf.kerberos



# Initialize kereberos with principals and keytables
#
kdb5_util create -s -P testpass

HOST="master.example.com"
KEYTABS="/etc/hadoop/conf.kerberos/master-keytabs"
kadmin.local -q "addprinc -randkey hdfs/$HOST@EXAMPLE.COM"
kadmin.local -q "addprinc -randkey mapred/$HOST@EXAMPLE.COM"
kadmin.local -q "addprinc -randkey yarn/$HOST@EXAMPLE.COM"
kadmin.local -q "addprinc -randkey HTTP/$HOST@EXAMPLE.COM"
mkdir "$KEYTABS"
kadmin.local -q "xst -norandkey -k $KEYTABS/hdfs.keytab hdfs/$HOST HTTP/$HOST"
kadmin.local -q "xst -norandkey -k $KEYTABS/mapred.keytab mapred/$HOST HTTP/$HOST"
kadmin.local -q "xst -norandkey -k $KEYTABS/yarn.keytab yarn/$HOST HTTP/$HOST"
kadmin.local -q "xst -norandkey -k $KEYTABS/HTTP.keytab HTTP/$HOST"
chown hdfs:hadoop $KEYTABS/hdfs.keytab
chown mapred:hadoop $KEYTABS/mapred.keytab
chown yarn:hadoop $KEYTABS/yarn.keytab
chown hdfs:hadoop $KEYTABS/HTTP.keytab
chmod 440 $KEYTABS/*.keytab

kadmin.local -q "addprinc -pw adminpass root/admin"
kadmin.local -q "addprinc -pw testpass dask"
kadmin.local -q "addprinc -pw testpass alice"
kadmin.local -q "addprinc -pw testpass bob"
kadmin.local -q "xst -norandkey -k /home/dask/dask.keytab dask HTTP/master.example.com"
chown dask:dask /home/dask/dask.keytab
chmod 440 /home/dask/dask.keytab
