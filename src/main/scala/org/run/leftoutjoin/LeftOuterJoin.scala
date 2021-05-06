package org.run.leftoutjoin

import org.apache.spark.{SparkConf, SparkContext}

object LeftOuterJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LeftOuterJoin").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val usersRaw = sc.textFile("I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap04\\scala\\users.tsv")
    val transactionRaw = sc.textFile("I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap04\\scala\\transactions.tsv")

    val users = usersRaw.map(line => {
      val tokens = line.split("\t")
      (tokens(0), ("L", tokens(1))) // Tagging Locations with L
    })

    val transactions = transactionRaw.map(line => {
      val tokens = line.split("\t")
      (tokens(2), ("P", tokens(1))) // Tagging Products with P
    })

    val all = users union transactions
    val grouped = all.groupByKey()

    val productLoactions = grouped.flatMap{
      case (userId, iterable) =>{
        //生成两个集合，一个包含L，一个包含P
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
