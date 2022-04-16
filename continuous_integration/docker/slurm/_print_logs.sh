#!/usr/bin/env bash

# Bold high intensity green
G='\033[1;92m'
# No color
NC='\033[0m'

printf "\n${G}supervisorctl status${NC}\n"
supervisorctl status

printf "\n${G}cat /var/log/supervisord.log${NC}\n"
cat /var/log/supervisord.log
printf "\n${G}cat /var/log/supervisor/slurmdbd.log${NC}\n"
cat /var/log/supervisor/slurmdbd.log
printf "\n${G}cat /var/log/supervisor/slurmctld.log${NC}\n"
cat /var/log/supervisor/slurmctld.log
