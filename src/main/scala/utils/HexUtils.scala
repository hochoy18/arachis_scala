package utils

/**
 * Created by Tongzhenguo on 2017/5/27.
 */
object HexUtils {
  def byte2hex(b: Array[Byte]): String = {
    var sb: StringBuffer = new StringBuffer
    var tmp: String = ""
    for( i <- b.indices ){
      tmp = Integer.toHexString(b(i) & 0XFF)
      if (tmp.length == 1) {
        sb.append("0" + tmp)
      }
      else {
        sb.append(tmp)
      }

    }
    sb.toString
  }

  def hex2byte(str: String): Array[Byte] = {
    if (str == null || str.trim.length==0 || str.trim.length%2==1 ) {
       null
    }

    val str_ = str.trim
    val len: Int = str_.length
    val b: Array[Byte] = new Array[Byte](len / 2)
    try {
      for (i <- Range(0, len, 2)) {
        b(i / 2) = Integer.decode("0X" + str_.substring(i, i + 2)).toByte
      }
      b
    }
    catch {
      case e: Exception => null
    }
  }

}
