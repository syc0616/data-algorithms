package org.run

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap


object TopNScala {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TopN").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val N = sc.broadcast(10)
    val inputPath = "I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap03\\scala\\sample_input\\*";

    val input = sc.textFile(inputPath)
    //生成元组
    val pair = input.map(line=>{
      val tokens = line.split(",")
      (tokens(2).toInt, tokens)
    })

    import Ordering.Implicits._
    val partitions = pair.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, Array[String]]
      itr.foreach { tuple =>
        { //各个分区添加
          sortedMap += tuple
          if (sortedMap.size > N.value) {
            //从右往左取N个数据
            sortedMap = sortedMap.takeRight(N.value)
          }
        }
      }
      sortedMap.takeRight(N.value).toIterator
    })

    // 收集所有分区
    val alltop10 = partitions.collect()
    val finaltop10 = SortedMap.empty[Int, Array[String]]
    // 添加失败
    //finaltop10 ++ alltop10
    for( (key, value) <- alltop10){
      finaltop10 + (key -> value)
    }
    val resultUsingMapPartition = finaltop10.takeRight(N.value)

    //Prints result (top 10) on the console
    resultUsingMapPartition.foreach {
      case (k, v) => println(s"-----$k \t ${v.mkString(",")}")
    }

    // Below is additional approach which is more concise
    val moreConciseApproach = pair.groupByKey().sortByKey(false).take(N.value)

    //Prints result (top 10) on the console
    moreConciseApproach.foreach {
      case (k, v) => println(s"#####$k \t ${v.flatten.mkString(",")}")
    }
  }
}
