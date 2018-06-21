/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package demo.spark.mllib

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import scala.Range

/**
 * An example Latent Dirichlet Allocation (LDA) app. Run with
 * {{{
 * ./bin/run-example mllib.LDAExample [options] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object LDAExample {

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Gaussian Mixture Model EM").setMaster("local[4]")
        val sc = new SparkContext(conf)
        // 输入的文件每行用词频向量表示一篇文档
        val data = sc.textFile("C:\\Users\\qingjian\\Desktop\\data.txt")
//        val data = sc.textFile("C:\\Users\\qingjian\\Desktop\\sample_lda_data.txt")
        val parsedData = data.map(s => Vectors.dense(s.trim.split(",").map(_.toDouble)))
        val corpus = parsedData.zipWithIndex.map(_.swap).cache()

        val ldaModel = new LDA().setK(2).run(corpus)

        // 打印主题
        println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
        val topics = ldaModel.topicsMatrix
        println(topics)
        
        for (topic <- Range(0, 2)) {
            print("Topic " + topic + ":")
            for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
            println()
        }
        
         
        
        
        
    }

}
// scalastyle:on println
