package utils

/**
 * Created by arachis on 2016/11/14.
 */
object VaildLogUtils extends Serializable {
  def isNumeric(str: String): Boolean = {
    var flag = true
    if ("".equals(str) || str == null) {
      flag = flag && false
    }
    for (i <- 0 to str.length - 1) {
      if (!Character.isDigit(str.charAt(i))) {
        flag = flag && false
      }
    }
    flag
  }

}
