#! /bin/bash

#for filename in *.sql
#do
#echo "Executing file $filename..."
#sqlplus FFOGLIA/Piaggine_3801096736@//localhost:1521/XE <<EOF
#@$filename;
#quit;
#EOF
#done
cd /c/flyway-6.2.2
flyway migrate
