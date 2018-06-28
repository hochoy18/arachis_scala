package utils

import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  *
  * Created by tongzhenguo on 2018/6/28.
  * 将spark sql DataFrame 换成libsvm格式
  */
object LibSVMFormater {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    // 测试离散化分区方法
    var input: DataFrame = spark.sparkContext.parallelize(
      0 until 100).map(_.toString).toDF(
      "id").withColumn(
      "a", rand(seed = 10)
    ).withColumn("b", rand(seed = 27)
    )
    input.createOrReplaceTempView("spark_tmp_table")
    val sql =
      """
        |select id%2 as label
        | ,a
        | ,b
        |from spark_tmp_table
      """.stripMargin
    input = spark.sql(sql)
    val retDF = run(input,"label",spark)
  }

  def run(inputDF:DataFrame,labelCol:String,spark:SparkSession): DataFrame ={

    var input = inputDF.na.drop()
    val labelIdx = input.columns.indexOf(labelCol)
    val baseIdx = 0 //libsvm index起始值
    val sep = " " // libsvm 特征分隔符
    val libsvmdata = input.rdd.map(row => {
      val label = row.get(labelIdx).toString.toDouble
      var libsvmline = s"$label$sep"
      for (idx <- 0 until row.length if idx != labelIdx) {
        val value = row.get(idx).toString.toDouble
        val index = baseIdx + idx
        libsvmline += s"$index:$value$sep"
      }
      libsvmline = libsvmline.trim
      Row(libsvmline)
    })
    val schema = StructType(List(StructField("libsvm", StringType, nullable = false)))
    val retDF = spark.createDataFrame(libsvmdata,schema)

    retDF.show()
    println(retDF.rdd.first()(0))
    // 0.0 1:0.41371264720975787 2:0.714105256846827
    retDF
  }

}
