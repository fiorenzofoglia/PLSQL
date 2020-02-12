#! /bin/bash

for filename in *.sql
do
echo "Executing file $filename..."
sqlplus -s FFOGLIA/Piaggine_3801096736//localhost:1521/XE as sysdba <<EOF
@$filename;
quit;
EOF
done
