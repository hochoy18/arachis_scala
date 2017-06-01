package utils

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
 * Created by Tongzhenguo on 2017/5/31.
 */
object JsonUtils extends Serializable{

  def parseArray( jsonArrayStr:String,access_time:Long ):List[Pair[Long, Map[String,Any ]]] ={
    val b = JSON.parseFull(jsonArrayStr)
    var set = mutable.HashSet[Pair[Long, Map[String,Any ]]]()
    val list: List[Pair[Long, Map[String, Any]]] = b match {

      case Some(list: List[Map[String, Any]] ) => {
        for (jmap <- list) {
          set += Tuple2(access_time, jmap)
        }
        set.toList
      }
      case other => set.toList
    }
    list
  }


}
