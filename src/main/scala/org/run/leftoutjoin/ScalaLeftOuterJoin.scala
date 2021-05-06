package org.run.leftoutjoin

import org.apache.spark.{SparkConf, SparkContext}

object ScalaLeftOuterJoin {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("leftjoin").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val usersRaw = sc.textFile("I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap04\\scala\\users.tsv")
    val transactionRaw = sc.textFile("I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap04\\scala\\transactions.tsv")
    //userid, location
    val users = usersRaw.map(line =>{
      val tokens = line.split("\t")
      (tokens(0), tokens(1))
    })
    users.foreach(f=>{
      println(f._1 + "," + f._2)
    })
    //userid, product
    val transaction = transactionRaw.map(line=>{
      val tokens = line.split("\t")
      (tokens(2), tokens(1))
    })

    val joined = transaction.leftOuterJoin(users)
    val productLocations = joined.values.map(f=>{
      (f._1, f._2.getOrElse("unknown"))
    })
    productLocations.foreach(f=>{
      println(f._1 + ", " +f._2)
    })

    val productByLocations = productLocations.groupByKey()

    val productWithUniqueLocations = productByLocations.mapValues(_.toSet)
    val result = productWithUniqueLocations.map(t=>(t._1, t._2.size))

    result.foreach(f=>{
      println(f._1 + "," + f._2)
    })


  }
}
