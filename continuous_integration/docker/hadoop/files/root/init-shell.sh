#!/bin/bash

# Wait for fixuid to have finished
if [[ "$HADOOP_TESTING_FIXUID" != "" ]]; then
    while [[ ! -f /var/run/fixuid.ran ]]
    do
        sleep 1
    done
fi

# cd into home directory
cd

# initialize as if login shell
if [ -f .bash_profile ]; then
    source .bash_profile;
fi
