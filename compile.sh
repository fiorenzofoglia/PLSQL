#! /bin/bash

for filename in *.sql
do
echo "Executing file $filename..."
sqlplus -s sys/GetStarted18c@//localhost:1521/XE as sysdba <<EOF
@$filename;
quit;
EOF
done
