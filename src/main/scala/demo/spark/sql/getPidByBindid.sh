#!/bin/bash
#bash ./getPidByBindid.sh /data/rd/wgl/anti-strategy/php/offline/script/gs/zhenshen.txt /data/rd/wgl/anti-strategy/php/offline/script/gs
scp $1 anti@bigdata-arch-client00.bh:/data/anti/wangguangliang/data/
file=${1##*/}
ssh anti@bigdata-arch-client00.bh "source /etc/profile && bash /data/anti/wangguangliang/run_getPidByBindid.sh /data/anti/wangguangliang/data/${file} /data/anti/wangguangliang/output/"
ssh anti@bigdata-arch-client00.bh "cat /data/anti/wangguangliang/output/wx_bindid" > $2/wx_bindid
ssh anti@bigdata-arch-client00.bh "cat /data/anti/wangguangliang/output/zm_bindid" > $2/zmxy_bindid
ssh anti@bigdata-arch-client00.bh "cat /data/anti/wangguangliang/output/qq_bindid" > $2/qq_bindid

