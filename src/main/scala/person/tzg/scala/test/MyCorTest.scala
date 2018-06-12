package person.tzg.scala.test

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.rand

import scala.collection.mutable.ArrayBuffer

/**
  *
  * Created by tongzhenguo on 2018/6/12.
  * 构造相关性矩阵测试
  */
object MyCorTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    val input: DataFrame = spark.sparkContext.parallelize(
      0 until 10).toDF(
      "id").withColumn(
      "a", rand(seed=10)
    ).withColumn("b", rand(seed=27)
    ).drop("id")
    val columns = Array("a","b")

    var fields = input.columns.map(col => StructField(col, DoubleType, nullable = false))
    fields = StructField("", StringType, nullable = false) +: fields
    val schema = StructType(fields)
    val values = ArrayBuffer[String]()
    for(col1 <- columns) {
      var line = new StringBuilder(columns.length)
      val arr = ArrayBuffer[Double]()
      for (col2 <- columns) {
        val cor = input.stat.corr(col1, col2)
        arr += cor
      }
      line = arr.addString(line,",")
      values += "%s,%s".format(col1 ,line.toString())
    }

    val rowRDD = spark.sparkContext.parallelize(
      values
    ).map(
      _.split(",")
    ).map(
      attributes => Row(attributes(0) +: attributes.tail.map(_.toDouble):_*)
    )

    val retDF = spark.createDataFrame(rowRDD, schema)
    retDF.show()
  }

}
