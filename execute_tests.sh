#! /bin/bash

sqlplus -s sys/GetStarted18c@//localhost:1521/XE as sysdba <<EOF
set serveroutput on;
execute ut3.ut.run('SYS');
quit;
EOF
