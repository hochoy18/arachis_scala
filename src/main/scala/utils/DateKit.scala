package utils

import java.text.{SimpleDateFormat, ParseException}
import java.util.{Locale, Date}


/**
 * Created by Tongzhenguo on 2017/5/27.
 */
object DateKit extends Serializable {
  @throws(classOf[ParseException])
  def parse(source: String, pattern: String, locale: Locale): Date = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(pattern, locale)
    return sdf.parse(source)
  }

  def main(args: Array[String]) {
    DateKit.parse("[11/May/2015:03:51:37 +0800]", "[dd/MMM/yyyy:HH:mm:ss Z]", Locale.US);

  }

}
