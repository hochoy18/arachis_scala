package tf

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.sql.SparkSession
import org.tensorflow.example._
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat

/**
  *
  * Created by tongzhenguo on 2018/7/9.
  * desc:
  *   将其他样本格式转化为TFRecords 格式
  */
object TFRecordsConvertUserCase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .getOrCreate()

    // 转换spark自带的数据集 sample_libsvm_data.txt
    val conf = Map(
      "delimiter" -> " "
      , "srcPath" -> "data/sample_libsvm_data.txt"
      , "tarPath" -> "data/tfrecords"
    )
    convert(spark,conf)
    
  }
  
  
  def convert(spark:SparkSession,conf:Map[String,String]): Unit = {

    val sc = spark.sparkContext
    val delimiter = conf("delimiter")
    val srcPath = conf("srcPath")
    val tarPath = conf("tarPath")

    sc.textFile(srcPath).map(lib2tfrecords(_, delimiter)).map(
      builder=>(new BytesWritable(builder.toByteArray), NullWritable.get())
    ).saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](tarPath)

  }

  def lib2tfrecords(line: String, delimiter: String): Example = {
    val features = Features.newBuilder()
    val parts = line.split(delimiter)
    val label = Int64List.newBuilder().addValue(parts(0).toLong).build()
    features.putFeature("label", Feature.newBuilder().setInt64List(label).build())
    parts.drop(1).map(_.split(":")).map { case Array(idx, value) =>
      if (value.forall(_.isDigit)) {
        val v = Int64List.newBuilder().addValue(value.toLong).build()
        features.putFeature(idx, Feature.newBuilder().setInt64List(v).build())
      } else {
        val v = FloatList.newBuilder().addValue(value.toFloat).build()
        features.putFeature(idx, Feature.newBuilder().setFloatList(v).build())
      }
    }
    val example: Example = Example.newBuilder().setFeatures(features.build()).build()
    example
  }
  

}
