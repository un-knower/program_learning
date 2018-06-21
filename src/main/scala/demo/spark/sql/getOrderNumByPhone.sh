#!/bin/bash
#bash ./getOrderNumByPhone.sh /data/rd/wgl/anti-strategy/php/offline/script/gs/passenger_phone.txt /data/rd/wgl/anti-strategy/php/offline/script/gs 20160302 20160302 20160303

scp $1 anti@bigdata-arch-client00.bh:/data/anti/wangguangliang/data/
file=${1##*/}
ssh anti@bigdata-arch-client00.bh "source /etc/profile && bash /data/anti/wangguangliang/run_getOrderNumByPhone.sh /data/anti/wangguangliang/data/${file} /data/anti/wangguangliang/output/ $3 $4 $5"
#ssh anti@bigdata-arch-client00.bh "bash /data/anti/wangguangliang/run_getOrderNumByPhone.sh ~/zengruhong/test/passenger_phone.txt ~/zengruhong/test 20160302 20160302 20160303"
ssh anti@bigdata-arch-client00.bh "cat /data/anti/wangguangliang/output/phone_order_number" > $2/phone_order_number

