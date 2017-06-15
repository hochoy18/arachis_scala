package utils

import com.fasterxml.jackson.databind.{ObjectMapper, ObjectWriter}

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
 * Created by Tongzhenguo on 2017/5/31.
 */
object JsonUtils extends Serializable{

  private var mapper: ObjectMapper = new ObjectMapper

  def parseArray( jsonArrayStr:String,access_time:Long ):List[Pair[Long, Map[String,Any ]]] ={
    JSON.globalNumberParser = { i:String=>i.toDouble }
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

  /**
   * 将对象转换为json字符串
   *
   * @param obj
   * @return 正常返回json字符串，否则返回""
   */
  def object2Json(obj: AnyRef): String = {
    if (obj == null) {
      return ""
    }
    try {
      val ow: ObjectWriter = mapper.writer
      return ow.writeValueAsString(obj)
    }
    catch {
      case e: Exception => {
        return ""
      }
    }
  }


//  /**
//   * 将json字符串转换为对象
//   *
//   * @param json
//   * @param clazz 对象的class
//   * @return 正常返回转换后的对象，否则返回null,所以使用了Option进行包装
//   */
//  def json2Object[Option[T]](json: String, clazz: Class[T]): Option[T] = {
//    if (json == null || ("" == json)) {
//      return None
//    }
//    try {
//      return Some(mapper.readValue(json, clazz))
//    }
//    catch {
//      case e: Exception => {
//        return None
//      }
//    }
//  }

  def main(args: Array[String]) {
    println( JsonUtils.object2Json( List(1,2,3,5) ) )
//    println( JsonUtils.json2Object(JsonUtils.object2Json( List(1,2,3,5) ), classOf[List]) )


  }

}
