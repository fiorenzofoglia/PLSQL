#! /bin/bash

rm report.xml
rm temp.txt

sqlplus sys/Piaggine_3801096736@//localhost:1521/XE as sysdba <<EOF
set serveroutput on;
execute ut3.ut.run('FFOGLIA', ut_xunit_reporter());
quit;
EOF >> report.xml

head -n -3 report.xml > temp.txt ; mv temp.txt report.xml
