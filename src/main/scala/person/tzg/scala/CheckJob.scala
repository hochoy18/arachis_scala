package person.tzg.scala

import org.apache.spark.{SparkConf, SparkContext}
import utils.DateUtils

/**
 * Created by arachis on 2016/11/14.
 */
object CheckJob extends Serializable {
  def main(args: Array[String]) {
    import java.util.Date
    val now: String = DateUtils.toDateString(DateUtils.plusDays(new Date, -1))
    val today: String = DateUtils.toDString(new Date)
    val logNow: String = today.substring(2, today.length)
    val path = "/tmp/log/" + now
    val sc = new SparkContext(new SparkConf())
    sc.wholeTextFiles(path).filter(l => {
      val filename: String = l._1
      var filecontent: String = l._2
      filecontent = filecontent.replaceAll("BindException", "")
      filecontent.toUpperCase().contains("ERROR") || filecontent.toUpperCase().contains("EXCEPTION")
    }).map(t2 => {
      t2._1 + ":\n" + t2._2
    })

  }


}
