package org.run.leftoutjoin

import org.apache.spark.sql.{Row, SparkSession}

object LeftOutJoinWithDataFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrame").master("lcoal").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.types._

    // Define user schema
    val userSchema = StructType(Seq(
      StructField("userId", StringType, false),
      StructField("location", StringType, false)))

    // Define transaction schema
    val transactionSchema = StructType(Seq(
      StructField("transactionId", StringType, false),
      StructField("productId", StringType, false),
      StructField("userId", StringType, false),
      StructField("quantity", IntegerType, false),
      StructField("price", DoubleType, false)))

    def userRows(line: String): Row = {
      val tokens = line.split("\t")
      Row(tokens(0), tokens(1))
    }

    def transactionRows(line: String): Row = {
      val tokens = line.split("\t")
      Row(tokens(0), tokens(1), tokens(2), tokens(3).toInt, tokens(4).toDouble)
    }

    val usersRaw = sc.textFile("I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap04\\scala\\users.tsv") // Loading user data
    // Converting to RDD[org.apache.spark.sql.Row]
    val userRDDRows = usersRaw.map(userRows(_))
    // obtaining DataFrame from RDD
    val users = spark.createDataFrame(userRDDRows, userSchema)

    val transactionsRaw = sc.textFile("I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap04\\scala\\transactions.tsv") // Loading transactions data
    // Converting to  RDD[org.apache.spark.sql.Row]
    val transactionsRDDRows = transactionsRaw.map(transactionRows(_))
    // obtaining DataFrame from RDD
    val transactions = spark.createDataFrame(transactionsRDDRows, transactionSchema)

    // performing join on on userId
    val joined = transactions.join(users, transactions("userId") === users("userId"))
    joined.printSchema()

    // Selecting only productId and location
    val product_location = joined.select(joined.col("productId"), joined.col("location"))
    // Getting only disting values
    val product_location_distinct = product_location.distinct
    val products = product_location_distinct.groupBy("productId").count()




  }
}
