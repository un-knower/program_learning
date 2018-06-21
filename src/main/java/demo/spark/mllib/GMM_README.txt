02 20 * * * source /etc/profile && sh anomaly/run_gmm.sh antispam.antispam__zc_order_merger_result kc > /tmp/run_gmm_kc.log 2>&1
03 21 * * * source /etc/profile && sh anomaly/run_gmm.sh antispam.antispam__feature_center_sf sf > /tmp/run_gmm_sf.log 2>&1
55 21 * * * source /etc/profile && sh anomaly/run_gmm.sh antispam.antispam__feature_center_taxi taxi > /tmp/run_gmm_taxi.log 2>&1

建表
CREATE TABLE test.antispam_cblof(
`create_time` String COMMENT '时间',
`kc` string COMMENT 'kc异常率',
`taxi` string COMMENT 'taxi异常率',
`sf` string COMMENT 'sf异常率'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' ; 
 
CREATE TABLE test.antispam_cblof_kc(
`time` String COMMENT '时间',
`cblof` string COMMENT 'kc异常率'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' ; 
 
CREATE TABLE test.antispam_cblof_sf(
`time` String COMMENT '时间',
`cblof` string COMMENT 'sf异常率'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' ; 


CREATE TABLE test.antispam_cblof_taxi(
`time` String COMMENT '时间',
`cblof` string COMMENT 'taxi异常率'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' ;  



查询hive

select * from test.antispam_cblof;

//授予权限
grant all on table  test.antispam_cblof to user wangleikidding;

显示权限
show grant user wangleikidding on table test.antispam_cblof;

//授予hdfs权限，相当于授予hive权限
hadoop fs -chmod 777 /user/wangguangliang/warehouse/cblof.db/antispam_cblof/


//删除数据
insert overwrite table test.antispam_cblof select * from test.antispam_cblof order by create_time;


 
GmmEM.jar 运行参数说明

(1) -i, --train 输入训练集 [必选项] 
(2) -u, --extraIdxTrain 训练数据的附加信息索引 0,1,3 
(3) -v, --dataIdxTrain 训练数据的参与聚类数据的索引 0,1,3 [必选项] 
(4) -w, --labelIdxTrain 训练数据的class信息索引 0,1,3 (5) -t, --test 输入测试集 [可选项] 
(6) -x, --extraIdxTest 测试数据的附加信息索引 0,1,3 
(7) -y, --dataIdxTest 测试数据的参与聚类数据的索引 0,1,3,如果extraIdxTest存在，则必须存在 
(8) -z, --labelIdxTest 测试数据的class信息索引 0,1,3 
(9) -o, --output 输入结果保存hdfs路径，[可选项] 
(10) -k, --k 指定聚类个数，默认-1，按loglikelihood自动选定聚类个数，[可选项] 
(11) -s, --separator 指定数据切分字符，默认 \t [可选项] 
(12) -n, --numIterations 指定迭代次数，默认10次 [可选项] 
(13) -a, --alpha 指定大聚类所占的比例，默认 0.9 [可选项] 
(14) -b, --beta 划分大小聚类的距离，默认 5 [可选项] 
(15) -h, --help 打印帮助信息 [可选项] 
(16) -p, --abnormal 保存异常类数据 [可选项] 
(17) -d, --day 查询hive数据日期，默认昨天 格式YYYYMMdd
(18) -e 存储异常率的hive表名 
(19) -f --flag 产品线名称，包括 sf 、taxi、 kc


查看数据

hive -e "
select oid,city_id,first_order_flag,first_order_ratio,non_local_flag,
non_local_ratio,passenger_group_size,group_order_count,trade_index,spamflag 
from antispam.antispam__zc_order_merger_result 
where concat(year,month,day)=20161010" > 1010.data


bazel编译
bazel build java/com/didi/anomaly:all



输入sql数据：

select oid,city_id,first_order_flag,first_order_ratio,non_local_flag,non_local_ratio,passenger_group_size,group_order_count,trade_index,spamflag 
from 
(
select oid,city_id,first_order_flag,first_order_ratio,non_local_flag,non_local_ratio,passenger_group_size,group_order_count,trade_index,spamflag,
row_number() over (partition by oid order by aegis_mtime desc)num 
from antispam.antispam__zc_order_merger_result where concat(year,month,day)=20161001
) t 
where t.num=1


hadoop 数据格式
((oid,[first_order_flag,first_order_ratio,non_local_flag,non_local_ratio,passenger_group_size,group_order_count,trade_index,spamflag],spamflag,预测聚类),异常值)
