#! /bin/bash

rm report.xml
rm temp.txt

sqlplus -s sys/GetStarted18c@//localhost:1521/XE as sysdba <<EOF
set serveroutput on;
SPOOL report.xml
execute ut3.ut.run('SYS', ut_xunit_reporter());
SPOOL OFF
quit;
EOF

head -n -1 report.xml > temp.txt ; mv temp.txt report.xml
