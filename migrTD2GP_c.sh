#!/bin/sh
read USR_TD PWD_TD < /aprun/shell/teradata/.apusr1
read USR_GP PWD_GP < /aprun/shell/greenplum/.shusr1

YYMMDD=
if [ $# -eq 2 ]; then
  YYMMDD=$2
else
  YYMMDD=`date +%Y%m%d`
fi

bteq .logon HMONIT_TD/$USR_TD,$PWD_TD <<EOF
.set width 65531;
.os rm /tmp/$1.csv;
.export file=/tmp/$1.csv;
.set separator '|';
select * from db_owner.$1 where $1_yymmdd = '$YYMMDD';
.quit;
EOF

tail -n +3 /tmp/$1.csv | iconv -f BIG-5 -t UTF-8 > /tmp/$1_gp.csv
#convert teradata's null to greenplum
#sed -in 's/?/\\N/g; s/[ ]*\\N[ ]*/\\N/g' /tmp/$1_gp.csv
sed -in 's/[ ]*?[ ]*/\\N/g' /tmp/$1_gp.csv

PGPASSWORD=$PWD_GP psql -d svelgp -h 192.168.178.109 -U $USR_GP <<EOF
delete from db_owner.$1 where $1_yymmdd = '$YYMMDD';
\copy db_owner.$1 from '/tmp/$1_gp.csv' with delimiter as '|'
EOF
