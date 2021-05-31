package org.run.topn

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap


object TopN {

  def main(args: Array[String]): Unit = {
    if (args.size < 1){
      println("Usage: TopN <input>")
      sys.exit(1)
    }


    val sparkConf = new SparkConf().setMaster("local").setAppName("TopN")

    val sc = new SparkContext(sparkConf)

    // 将变量广播出去
    val bcN = sc.broadcast(10)
    val path = args(0)

    val input = sc.textFile(path)
    val pair = input.map(line =>{
      val tokens = line.split(",")
      (tokens(2).toInt, tokens)
    })
    // 求每个分区的TOPN
    val partitions = pair.mapPartitions(itr=>{
      var sortedMap = SortedMap.empty[Int, Array[String]]

      itr.foreach(tuple=>{
        //向map中添加数据，同时排序默认是升序
        sortedMap += tuple
        if (sortedMap.size > bcN.value){
          // 取最右边的N个数据
          sortedMap = sortedMap.takeRight(bcN.value)
        }
      })
      // 返回迭代器
      sortedMap.takeRight(bcN.value).toIterator
    })


    // 汇总最后的TOPN，抽回到Driver端
    val alltop10 = partitions.collect()
    // 最终的TOPN
    val finaltop10 = SortedMap.empty[Int, Array[String]].++:(alltop10)
    val resultUsingMapPartition = finaltop10.takeRight(bcN.value)

    // 打印
    // 偏函数{ case() => {} }
    resultUsingMapPartition.foreach {
      case (k, v) => println(s"$k \t ${v.asInstanceOf[Array[String]].mkString(",")}")
    }



    //第二种方法，一句话搞定
    val moreConciseApproach = pair.groupByKey().sortByKey(false).take(bcN.value)
    moreConciseApproach.foreach{
      // 偏函数，flatten就是将多个集合展开，组合成新的一个集合
      case (k,v) => println(s"$k \t ${v.flatten.mkString(",")}")
    }


    sc.stop()

  }
}
