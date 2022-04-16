#!/usr/bin/env bash

# Bold high intensity green
G='\033[1;92m'
# No color
NC='\033[0m'

printf "\n${G}supervisorctl status${NC}\n"
supervisorctl status

printf "\n${G}cat /var/log/supervisor/krb5kdc.log${NC}\n"
cat /var/log/supervisor/krb5kdc.log
printf "\n${G}cat /var/log/supervisor/kadmind.log${NC}\n"
cat /var/log/supervisor/kadmind.log
printf "\n${G}cat /var/log/supervisor/yarn-nodemanager.log${NC}\n"
cat /var/log/supervisor/yarn-nodemanager.log
printf "\n${G}cat /var/log/supervisor/yarn-resourcemanager.log${NC}\n"
cat /var/log/supervisor/yarn-resourcemanager.log
printf "\n${G}cat /var/log/supervisor/hdfs-namenode.log${NC}\n"
cat /var/log/supervisor/hdfs-namenode.log
printf "\n${G}cat /var/log/supervisor/hdfs-datanode.log${NC}\n"
cat /var/log/supervisor/hdfs-datanode.log
