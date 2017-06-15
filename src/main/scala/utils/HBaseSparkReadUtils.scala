/*
package utils

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Spark read from HBase
 * Created by arachis on 2016/12/2.
 */
object HBaseSparkReadUtils extends Serializable {

  import java.io.IOException
  import java.util

  import org.apache.hadoop.hbase.HBaseConfiguration
  import org.apache.hadoop.hbase.client.{HConnectionManager, Result, Scan}
  import org.apache.hadoop.hbase.io.ImmutableBytesWritable
  import org.apache.hadoop.hbase.mapreduce.TableInputFormat
  import org.apache.hadoop.hbase.protobuf.ProtobufUtil
  import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
  import org.apache.hadoop.hbase.util.{Base64, Bytes}
  import org.apache.spark.rdd.RDD

  val TABLE: String = "hbase_test"
  val USER_COL_NAME: String = "count"
  val ONE: Integer = new Integer(1)
  val F_MONTH: String = "PLAY-F_MONTH"
  //basically tells Spark not to ship it with the closure.
  //link :http://stackoverflow.com/questions/27181454/operating-rdd-failed-while-setting-spark-record-delimiter-with-org-apache-hadoop
  @transient val config = HBaseConfiguration.create()
  val ZK_QUORUM = "10.0.0.1,10.0.0.2"
  config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
  config.set("hbase.zookeeper.quorum", ZK_QUORUM);
  val connection = HConnectionManager.createConnection(config);
  val table = connection.getTable(TABLE);
  table.setAutoFlush(true, false);

  private val num: Int = 30
  private val maxdays: Int = 6

  def main(args: Array[String]) {
    doJob
  }

  @throws(classOf[IOException])
  def convertScanToString(scan: Scan): String = {
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    return Base64.encodeBytes(proto.toByteArray)
  }

  def doJob {
    val sparkConf: SparkConf = new SparkConf().setAppName("HbaseSparkUtils").setMaster("local[10]")
    val sc: SparkContext = new SparkContext(sparkConf)
    val scanCount: Scan = new Scan
    scanCount.setCaching(500)
    scanCount.addColumn(Bytes.toBytes("count"), Bytes.toBytes("sum"))
    scanCount.setStartRow(Bytes.toBytes("20160101-sum"))
    scanCount.setStopRow(Bytes.toBytes("20161023-sum"))
    try {
      config.set(TableInputFormat.INPUT_TABLE, TABLE)
      config.set(TableInputFormat.SCAN, convertScanToString(scanCount))
      val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      val count = hBaseRDD.flatMap(
        t2 => {
          val list: util.ArrayList[(String, Long)] = new util.ArrayList[(String, Long)]
          import scala.collection.JavaConversions._
          for (cell <- t2._2.listCells) {
            val value: String = new Predef.String(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
            if (VaildLogUtils.isNumeric(value)) {
              val `val`: Long = value.toLong
              list.add(new (String, Long)("count", `val`))
            }
          }
          list
        }
      ).reduceByKey {
        case (v1: Long, v2: Long) => v1 + v2
      }
      println("=================================================all  count:" + count.first()._2)

    }
    catch {
      case e: IOException => {
        e.printStackTrace
      }
    }
  }

}
*/
