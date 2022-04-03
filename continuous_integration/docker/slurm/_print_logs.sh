#!/usr/bin/env bash
set -x

printf "\n\n\n\n\n"
supervisorctl status

printf "\n\n\n\n\n"
cat /var/log/supervisord.log
printf "\n\n\n\n\n"
cat /var/log/supervisor/slurmdbd.log
printf "\n\n\n\n\n"
cat /var/log/supervisor/slurmctld.log
