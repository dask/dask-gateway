#!/usr/bin/env bash
set -ex

PBS_CONF_FILE=/etc/pbs.conf
MOM_CONF_FILE=/var/spool/pbs/mom_priv/config
HOSTNAME=$(hostname)

# Configure PBS to run all on one node
#
# Configuration references:
# - https://github.com/openpbs/openpbs/blob/master/doc/man8/pbs.conf.8B
# - https://github.com/openpbs/openpbs/blob/HEAD/doc/man8/pbs_comm.8B
# - https://github.com/openpbs/openpbs/blob/master/doc/man8/pbs_mom.8B
#
sed -i "s/PBS_SERVER=.*/PBS_SERVER=$HOSTNAME/" $PBS_CONF_FILE
sed -i "s/PBS_START_MOM=.*/PBS_START_MOM=1/" $PBS_CONF_FILE
sed -i "s/\$clienthost .*/\$clienthost $HOSTNAME/" $MOM_CONF_FILE
echo "\$usecp *:/ /" >> $MOM_CONF_FILE

# Reduce the memory footprint by using less threads to avoid the OOMKiller in
# GitHub Actions as observed with exit code 137.
#
echo "PBS_COMM_THREADS=2" >> $PBS_CONF_FILE

# Start PBS
/etc/init.d/pbs start

# Reduce time between PBS scheduling and add history
/opt/pbs/bin/qmgr -c "set server scheduler_iteration = 20"
/opt/pbs/bin/qmgr -c "set server job_history_enable = True"
/opt/pbs/bin/qmgr -c "set server job_history_duration = 24:00:00"
/opt/pbs/bin/qmgr -c "set node pbs queue=workq"
/opt/pbs/bin/qmgr -c "set server operators += dask@pbs"

# "Entering sleep" can be used as a signal in logs that we have passed the
# initialization phase where the memory needs may peak and expose us to the
# OOMKiller and 137 exit codes.
#
echo "Entering sleep"
sleep infinity
