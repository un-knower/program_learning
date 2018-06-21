package demo.spark.batch.customerorder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import java.util.*;

/**
 * top n
 * 可以使用top(N)方法
 * 如果不使用的话，如下实现，mapPartitions模仿MapReduce的思路
 */
public class TopN {
    private static final int N = 5;
    public static  void main(String args[]) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("top n").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<String> data = new ArrayList<>();
        data.add("5");
        data.add("10");
        data.add("1");
        data.add("6");
        data.add("3");
        data.add("8");
        data.add("9");
        data.add("19");

        JavaRDD<String> dataRdd = jsc.parallelize(data);
        Broadcast<Integer> broadcastN = jsc.broadcast(N);
        JavaRDD<SortedMap<Integer, String>> sortedMapJavaRDD = dataRdd.mapPartitions(new FlatMapFunction<Iterator<String>, SortedMap<Integer, String>>() {

            @Override
            public Iterator<SortedMap<Integer, String>> call(Iterator<String> dataIter) throws Exception {
                SortedMap<Integer, String> topN = new TreeMap<>();
                while (dataIter.hasNext()) {
                    String data = dataIter.next();
                    Integer dataInt = Integer.valueOf(data);
                    topN.put(dataInt, data);
                    // 如果超出了N，则删除最小值
                    if (topN.size() > broadcastN.getValue()) {
                        topN.remove(topN.firstKey());  // topN.remove(topN.lastKey()) 计算bottom n
                    }
                }
                return Collections.singletonList(topN).iterator();
            }
        });

        // 模拟MapReduce的单一任务reduce，方法1
        List<SortedMap<Integer, String>> allTopN = sortedMapJavaRDD.collect();
        SortedMap<Integer, String> finalTopN = new TreeMap<>();
        for (SortedMap<Integer, String> localTopN : allTopN) {
            for( Map.Entry<Integer, String> entries : localTopN.entrySet()) {
                finalTopN.put(entries.getKey(), entries.getValue());
                if (finalTopN.size() > N) {
                    finalTopN.remove(finalTopN.firstKey());  // .remove(topN.lastKey()) 计算bottom n
                }
            }
        }

        for(Map.Entry<Integer, String> entries : finalTopN.entrySet()) {
            System.out.println(entries.getKey()+":"+entries.getValue());
        }


        System.out.println("方法2：reduce方法");
        // 模拟MapReduce的单一任务reduce，方法2 使用reduce
        SortedMap<Integer, String> finalTopNResult = sortedMapJavaRDD.reduce(new Function2<
                SortedMap<Integer, String>,  // 输入
                SortedMap<Integer, String>,  // 输入
                SortedMap<Integer, String>>()  // 输出，最终结果
        {
            @Override
            public SortedMap<Integer, String> call(SortedMap<Integer, String> v1, SortedMap<Integer, String> v2) throws Exception {
                SortedMap<Integer, String> finalTopN = new TreeMap<>();
                // 处理v1
                for (Map.Entry<Integer, String> entries : v1.entrySet()) {
                    finalTopN.put(entries.getKey(), entries.getValue());
                    if (finalTopN.size() > broadcastN.getValue()) {
                        finalTopN.remove(finalTopN.firstKey());  // .remove(topN.lastKey()) 计算bottom n
                    }
                }
                // 处理v2
                for (Map.Entry<Integer, String> entries : v2.entrySet()) {
                    finalTopN.put(entries.getKey(), entries.getValue());
                    if (finalTopN.size() > broadcastN.getValue()) {
                        finalTopN.remove(finalTopN.firstKey());  // .remove(topN.lastKey()) 计算bottom n
                    }
                }
                return finalTopN;
            }
        });

        for(Map.Entry<Integer, String> entries : finalTopNResult.entrySet()) {
            System.out.println(entries.getKey()+":"+entries.getValue());
        }



        jsc.stop();
    }

}
