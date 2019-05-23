#! /bin/bash

create_keytabs() {
    HOST="$1.example.com"
    KEYTABS="/etc/hadoop/conf.kerberos/$1-keytabs"
    kadmin.local -q "addprinc -randkey hdfs/$HOST@EXAMPLE.COM" \
    && kadmin.local -q "addprinc -randkey mapred/$HOST@EXAMPLE.COM" \
    && kadmin.local -q "addprinc -randkey yarn/$HOST@EXAMPLE.COM" \
    && kadmin.local -q "addprinc -randkey HTTP/$HOST@EXAMPLE.COM" \
    && mkdir "$KEYTABS" \
    && kadmin.local -q "xst -norandkey -k $KEYTABS/hdfs.keytab hdfs/$HOST HTTP/$HOST" \
    && kadmin.local -q "xst -norandkey -k $KEYTABS/mapred.keytab mapred/$HOST HTTP/$HOST" \
    && kadmin.local -q "xst -norandkey -k $KEYTABS/yarn.keytab yarn/$HOST HTTP/$HOST" \
    && kadmin.local -q "xst -norandkey -k $KEYTABS/HTTP.keytab HTTP/$HOST" \
    && chown hdfs:hadoop $KEYTABS/hdfs.keytab \
    && chown mapred:hadoop $KEYTABS/mapred.keytab \
    && chown yarn:hadoop $KEYTABS/yarn.keytab \
    && chown hdfs:hadoop $KEYTABS/HTTP.keytab \
    && chmod 440 $KEYTABS/*.keytab
}

kdb5_util create -s -P testpass \
&& create_keytabs master \
&& kadmin.local -q "addprinc -pw adminpass root/admin" \
&& kadmin.local -q "addprinc -pw testpass dask" \
&& kadmin.local -q "addprinc -pw testpass alice" \
&& kadmin.local -q "addprinc -pw testpass bob" \
&& kadmin.local -q "xst -norandkey -k /home/dask/dask.keytab dask HTTP/master.example.com" \
&& chown dask:dask /home/dask/dask.keytab
