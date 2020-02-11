#! /bin/bash

for filename in *_TEST.sql
do
echo "Executing file $filename..."
sqlplus -s sys/GetStarted18c@//localhost:1521/XE as sysdba <<EOF
execute ut3.ut.run('$filename');
quit;
EOF
done
