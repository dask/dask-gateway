#!/usr/bin/env bash
set -x

printf "\n\n\n\n\n"
supervisorctl status

printf "\n\n\n\n\n"
cat /var/log/supervisor/krb5kdc.log
printf "\n\n\n\n\n"
cat /var/log/supervisor/kadmind.log
printf "\n\n\n\n\n"
cat /var/log/supervisor/yarn-nodemanager.log
printf "\n\n\n\n\n"
cat /var/log/supervisor/yarn-resourcemanager.log
printf "\n\n\n\n\n"
cat /var/log/supervisor/hdfs-namenode.log
printf "\n\n\n\n\n"
cat /var/log/supervisor/hdfs-datanode.log
