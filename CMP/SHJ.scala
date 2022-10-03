package com.quan.aliyunshiyan.twoJoinCompare
import com.quan.ConnectHbase.spark_hbase.calculate
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.col

/**
 * SHJ between two tables
 */
object SHJ {
  def main(args: Array[String]): Unit = {
    org.apache.log4j.PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val spark = SparkSession
      .builder()
      .appName("astdata")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.sqlContext.implicits._

    val inputdir1 = "F:\\大数据资料\\ast3数据\\AST3II_dr1_catalog\\jointxt\\Tess004_merge9\\AST3II-Tess004_1.txt"

    sc.textFile(inputdir1).map(t => {
      val arr = t.split(" ")
      (arr(0).toInt,arr(1), arr(2), arr(3), arr(4), arr(5),arr(6), arr(7),arr(8))})
      .toDF( "flag","id","ra", "dec", "mag","magerr","healpix","healpix_n","time")
      .createTempView("t1")

    val inputdir2 ="F:\\大数据资料\\ast3数据\\AST3II_dr1_catalog\\jointxt\\Tess004_merge9\\merge9_200time.txt"

    sc.textFile(inputdir2,9).map(t => {
      val arr = t.split(" ")
      (arr(0).toInt,arr(1), arr(2), arr(3), arr(4), arr(5),arr(6), arr(7),arr(8))})
      .toDF( "flag","id","ra", "dec", "mag","magerr","healpix","healpix_n","time")
      .createTempView("t2")

    val resjoin= spark.sql("SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 right join t2 on t1.healpix = t2.healpix;")
      .withColumn("dis", calculate(col("t1.ra"), col("t1.dec"), col("t2.ra"), col("t2.dec")))
      .filter($"dis" < 0.0000013888)

    resjoin.rdd.saveAsTextFile("data/joinout/" )
    sc.stop()
  }
}
