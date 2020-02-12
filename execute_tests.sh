#! /bin/bash

rm report.xml
rm temp.txt

sqlplus -s FFOGLIA/Piaggine_3801096736@//localhost:1521/XE as sysdba <<EOF
set serveroutput on;
SPOOL report.xml
execute ut3.ut.run('FFOGLIA', ut_xunit_reporter());
SPOOL OFF
quit;
EOF

head -n -3 report.xml > temp.txt ; mv temp.txt report.xml
