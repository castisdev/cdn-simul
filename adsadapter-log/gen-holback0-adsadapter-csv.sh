#!/bin/bash
OUT="holdback0-adsadapter.csv"

egrep "InsertSchedule,|FileTransfer,,TRANSACTION" 2017-05-*|cut -d',' -f3,4,5,6,7,8,9,10|sort > start.txt
i=0
while read first; read second;
do
	i=$((i+1))
	echo $first, $second, $i
	file=$(echo $first | awk '{print $3}')
	start=$(echo $second | cut -d',' -f1,2)
	echo "$start,$file" >> $OUT
done <start.txt
sort $OUT
rm -rf start.txt
