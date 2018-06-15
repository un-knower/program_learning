package demo.kafka.demo2;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 自定义partition
 * 
 */
public class MyPartition implements Partitioner {

    public MyPartition(VerifiableProperties verifiableProperties) {
        //记得要有这个构造函数，不然会报错！
    }

    public int partition(Object key, int numPartitions) {
        if( key == null ) return 0 ;
        Integer k = Integer.parseInt(key+"") ;
        return k % numPartitions;
    }
}
