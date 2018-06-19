package demo.spark.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ClusterPrecise {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val data = sc.textFile("files/cluster_precise.txt").map { line =>
            val lineArr = line.split("\\s+")
            //label     predictclass
            (lineArr(0), lineArr(1))
        }
        //data.foreach(println)
        val lev = 0
        val numClusters = 5
        val trueCluster = data.map(_._1).distinct().count.toInt
        val clusterTotals = new Array[Int](numClusters)
        val clusterTotalsResult = data.map(x => (x._2, 1)).reduceByKey(_ + _).sortBy(_._1).collect
        for(clusterTotal<-clusterTotalsResult) {
            clusterTotals(clusterTotal._1.toInt)=clusterTotal._2
        }

        val a = data.map(x => (x, 1)).reduceByKey(_ + _).sortBy(_._1).collect
        val counts = Array.ofDim[Int](numClusters, trueCluster)
        var startClust = ""
        var i = 0
        a.foreach { line =>
            if (startClust == "") {
                startClust = line._1._1
            } else if (startClust != line._1._1) {
                i += 1
                startClust = line._1._1
            }
            counts(line._1._2.toInt)(i) = line._2
        }
        
        val current = new Array[Double](numClusters + 1);
        val best = new Array[Double](numClusters + 1);
        best(numClusters) = Double.MaxValue
        val error = 0;
        mapClasses(numClusters, lev, counts, clusterTotals, current, best, error);
        for (d <- best) {
            println(d);
        }
        println("error="+best(best.length-1))
        //data.map(x=>(x,1)).reduceByKey(_+_)
        /*     scala版测试
        val numClusters=5; //聚了5类 
		val lev = 0;
//		val counts= Array(Array(28,0,0),Array(0,0,35),Array(0,27,15),Array(22,0,0),Array(0,23,0)); //每个聚类的个数
//		val clusterTotals=Array(28,35,42,22,23); 
		val counts= Array(Array(28,0,0,28,0),Array(0,0,35,0,35),Array(0,27,15,0,0),Array(22,0,0,0,0),Array(0,23,0,0,0)); //每个聚类的个数
		val clusterTotals=Array(56,70,42,22,23); 
		val current=new Array[Double](numClusters+1); 
		val best = new Array[Double](numClusters+1);
		best(numClusters)= Double.MaxValue
		val error=0;
		mapClasses(numClusters, lev, counts, clusterTotals, current, best, error);
		for ( d <- best) {
			println(d);
		}
*/
    }
    /**
     * Finds the minimum error mapping of classes to clusters. Recursively
     * considers all possible class to cluster assignments.
     *
     * @param numClusters
     *            the number of clusters
     * @param lev
     *            the cluster being processed
     * @param counts
     *            the counts of classes in clusters
     * @param clusterTotals
     *            the total number of examples in each cluster
     * @param current
     *            the current path through the class to cluster assignment tree
     * @param best
     *            the best assignment path seen
     * @param error
     *            accumulates the error for a particular path
     */
    def mapClasses(numClusters: Int, lev: Int, counts: Array[Array[Int]],
                   clusterTotals: Array[Int], current: Array[Double], best: Array[Double], error: Int) {
        // leaf
        if (lev == numClusters) {
            if (error < best(numClusters)) {
                best(numClusters) = error;
                for (i <- 0 until numClusters) {
                    best(i) = current(i);
                }
            }
        } else {
            // empty cluster -- ignore
            if (clusterTotals(lev) == 0) {
                current(lev) = -1; // cluster ignored
                mapClasses(numClusters, lev + 1, counts, clusterTotals,
                    current, best, error);
            } else {
                // first try no class assignment to this cluster
                current(lev) = -1; // cluster assigned no class (ie all errors)
                mapClasses(numClusters, lev + 1, counts, clusterTotals,
                    current, best, error + clusterTotals(lev));
                // now loop through the classes in this cluster
                for (i <- 0 until counts(0).length) {
                    if (counts(lev)(i) > 0) {
                        var ok = true;
                        // check to see if this class has already been assigned
                        for (j <- 0 until lev if (ok)) {
                            if (current(j).toInt == i) {
                                ok = false;
                            }
                        }
                        if (ok) {
                            current(lev) = i;
                            mapClasses(
                                numClusters,
                                lev + 1,
                                counts,
                                clusterTotals,
                                current,
                                best,
                                (error + (clusterTotals(lev) - counts(lev)(i))));
                        }
                    }
                }
            }
        }
    } //function mapClasses
}