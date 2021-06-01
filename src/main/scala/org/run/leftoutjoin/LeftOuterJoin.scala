package org.run.leftoutjoin

import org.apache.spark.{SparkConf, SparkContext}

object LeftOuterJoin {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LeftOuterJoin").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val usersRaw = sc.textFile("E:\\data\\input\\chapter4\\users.tsv")
    val transactionRaw = sc.textFile("E:\\data\\input\\chapter4\\transactions.tsv")

    val users = usersRaw.map(line => {
      val tokens = line.split("\t")
      (tokens(0), ("L", tokens(1))) // Tagging Locations with L
    })

    val transactions = transactionRaw.map(line => {
      val tokens = line.split("\t")
      (tokens(2), ("P", tokens(1))) // Tagging Products with P
    })

    // 两个RDD进union，前提是结构一样
    val all = users union transactions
    // 分组
    val grouped = all.groupByKey()


    val productLoactions = grouped.flatMap{ //偏函数
      case (userId, iterable) => {
        //生成两个集合，一个包含L，一个包含P --- (List((L,UT)),List((P,p3), (P,p1), (P,p1), (P,p4)))
        //从左向右应用条件p进行判断，直到条件p不成立，此时将列表分为两个列表
        val (location, products) = iterable span (_._1 == "L")
        //取location的第一个若没有"L"值，则赋值为Unkown
        val loc = location.headOption.getOrElse(("L", "UNKNOWN"))
        //先过滤第一个为"P"的，生成(product,local)，并对local去重
        products.filter(_._1 == "P").map(p => (p._2, loc._2)).toSet
      }

    }

    val productByLocations = productLoactions.groupByKey()
    // Return (product, location count) tuple
    val result = productByLocations.map(t => (t._1, t._2.size))
    result.foreach(f=>{
      println(f._1 + ", " + f._2)
    })
  }
}
