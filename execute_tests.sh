#! /bin/bash

sqlplus -s sys/GetStarted18c@//localhost:1521/XE as sysdba <<EOF
execute ut3.ut.run('SYS');
quit;
EOF
