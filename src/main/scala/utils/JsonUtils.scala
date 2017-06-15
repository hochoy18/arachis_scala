package utils

import java.io.IOException

import com.fasterxml.jackson.core.JsonGenerationException
import com.fasterxml.jackson.databind.{ObjectMapper, JsonMappingException}

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
   * 将json字符串转换为对象
   *
   * @param json
   * @param clazz 对象的class
   * @return 正常返回转换后的对象，否则返回null,所以使用了Option进行包装
   */
  def json2Object[Option[T]](json: String, clazz: Class[T]): T = {
    if (json == null || ("" == json)) {
      return None
    }
    try {
      return mapper.readValue(json, clazz)
    }
    catch {
      case e: Exception => {
        return None
      }
    }
  }

}
