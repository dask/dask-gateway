#!/usr/bin/env bash

yum install -y psmisc

if [ ! -f "/var/lib/mysql/ibdata1" ]; then
    echo "- Initializing database"
    /usr/bin/mysql_install_db &> /dev/null
    echo "- Database initialized"
    echo "- Updating MySQL directory permissions"
    chown -R mysql:mysql /var/lib/mysql
    chown -R mysql:mysql /var/run/mariadb
fi

if [ ! -d "/var/lib/mysql/slurm_acct_db" ]; then
    /usr/bin/mysqld_safe --datadir='/var/lib/mysql' &

    for count in {30..0}; do
        if echo "SELECT 1" | mysql &> /dev/null; then
            break
        fi
        echo "- Starting MariaDB to create Slurm account database"
        sleep 1
    done

    if [[ "$count" -eq 0 ]]; then
        echo >&2 "MariaDB did not start"
        exit 1
    fi

    echo "- Creating Slurm acct database"
    mysql -NBe "CREATE DATABASE slurm_acct_db"
    mysql -NBe "CREATE USER 'slurm'@'localhost'"
    mysql -NBe "SET PASSWORD for 'slurm'@'localhost' = password('password')"
    mysql -NBe "GRANT USAGE ON *.* to 'slurm'@'localhost'"
    mysql -NBe "GRANT ALL PRIVILEGES on slurm_acct_db.* to 'slurm'@'localhost'"
    mysql -NBe "FLUSH PRIVILEGES"
    echo "- Slurm acct database created. Stopping MariaDB"
    killall mysqld
    for count in {30..0}; do
        if echo "SELECT 1" | mysql &> /dev/null; then
            sleep 1
        else
            break
        fi
    done
    if [[ "$count" -eq 0 ]]; then
        echo >&2 "MariaDB did not stop"
        exit 1
    fi
fi

yum remove -y psmisc
rm -rf /var/cache/yum
