#!/bin/bash
OUT="normal-adsadapter.csv"

egrep "InsertSchedule,|Encryption|Multicast_Channel_IP|SetClientTree,,\"node info adcIP : 125.148.196.3|FileTransfer,,TRANSACTION" 2017-05-*|cut -d',' -f3,4,5,6,7,8,9,10|sort|grep 125.148.196.3 -C 2|egrep "InsertSchedule|TRANSACTION" > start.txt
i=0
while read first; read second;
do
	i=$((i+1))
	echo $first, $second, $i
	file=$(echo $first | awk '{print $3}')
	tid=$(echo $second |awk '{print $4}')
	end=$(grep "send transfer notification success" 2017-05-*| grep $tid| cut -d',' -f3,4)
	echo "$end,$file,$tid" >> $OUT
done <start.txt
sort $OUT
rm -rf start.txt
