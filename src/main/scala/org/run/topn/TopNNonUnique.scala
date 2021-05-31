package org.run.topn

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap

object TopNNonUnique {

  def main(args: Array[String]): Unit = {

    if(args.length < 1){
      println("Usage: TopNNonUnique <input>")
      sys.exit(1)
    }


    val sparkConf = new SparkConf().setAppName("TopNNonUnique").setMaster("local")

    val sc = new SparkContext(sparkConf)
    // 广播变量
    val bcN = sc.broadcast(10)

    val path = args(0)


    val input = sc.textFile(path)
    val kv = input.map(line=>{
      val tokens = line.split(",")
      // 隐式转换，强转
      //println(s"${tokens(1)} : , ${tokens(2)} : ")
      (tokens(1), tokens(2).toInt)
    })


    val uniqueKeys = kv.reduceByKey(_+_)
    import Ordering.Implicits._
    // 每个分区求topN
    val partitions = uniqueKeys.mapPartitions(itr=>{
      var sortedMap = SortedMap.empty[Int, String]
      itr.foreach{
        tuple =>{
          // 交换
          sortedMap += tuple.swap
          if (sortedMap.size > bcN.value){
            // 默认升序，取前几个
            sortedMap = sortedMap.takeRight(bcN.value)
          }
        }
      }
      sortedMap.takeRight(bcN.value).toIterator
    })

    // dirver端求topn
    val alltop10 = partitions.collect()
    val finaltop10 = SortedMap.empty[Int, String].++:(alltop10)

    val resultUsingMapPartition = finaltop10.takeRight(bcN.value)
    resultUsingMapPartition.foreach{
      case (k,v) => println(s"finaltop10 $k \t $v ")
    }

    // 初始值，没有时赋值一次
    val createCombiner = (v : Int) => v
    // 每个分区执行聚合代码
    val mergeValue = (a:Int, b:Int)=> (a + b)
    // driver端执行聚合代码

    val moreConciseApproach = kv.combineByKey(createCombiner,
      mergeValue, mergeValue)
        .map(_.swap)   //交换key和value
        .groupByKey()  //分组
        .sortByKey(false) //降序排序
        .take(bcN.value)


    moreConciseApproach.foreach{ //偏函数 { case } --- { } 不能省略
      case (k,v) => println(s"$k \t ${v.mkString(",")}")
    }

    sc.stop()

  }

}
