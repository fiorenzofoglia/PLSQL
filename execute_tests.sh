#! /bin/bash

rm report.xml

sqlplus -s sys/GetStarted18c@//localhost:1521/XE as sysdba <<EOF
set serveroutput on;
SPOOL report.xml
execute ut3.ut.run('SYS');
quit;
EOF
