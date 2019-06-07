#!/usr/bin/env bash

PBS_CONF_FILE=/etc/pbs.conf
MOM_CONF_FILE=/var/spool/pbs/mom_priv/config
HOSTNAME=$(hostname)

# Configure PBS to run all on one node
sed -i "s/PBS_SERVER=.*/PBS_SERVER=$HOSTNAME/" $PBS_CONF_FILE
sed -i "s/PBS_START_MOM=.*/PBS_START_MOM=1/" $PBS_CONF_FILE
sed -i "s/\$clienthost .*/\$clienthost $HOSTNAME/" $MOM_CONF_FILE
echo "\$usecp *:/ /" >> $MOM_CONF_FILE

# Start PBS
/etc/init.d/pbs start

# Reduce time between PBS scheduling and add history
/opt/pbs/bin/qmgr -c "set server scheduler_iteration = 20"
/opt/pbs/bin/qmgr -c "set server job_history_enable = True"
/opt/pbs/bin/qmgr -c "set server job_history_duration = 24:00:00"
/opt/pbs/bin/qmgr -c "set node pbs queue=workq"
/opt/pbs/bin/qmgr -c "set server operators += dask@pbs"

sleep infinity
