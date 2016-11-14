package utils

/**
 * Created by arachis on 2016/11/14.
 */
object DateUtils extends Serializable {

  import java.util.{Calendar, Date}

  import org.apache.commons.lang.time.FastDateFormat

  def plusDays(date: java.util.Date, days: Int): Date = {
    val cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.DAY_OF_MONTH, days);
    return cal.getTime();
  }

  val JAVA_DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd");
  val JAVA_D_FORMAT = FastDateFormat.getInstance("yyyy/MM/dd");

  def toDateString(dateTime: Date): java.lang.String = {
    if (dateTime == null) {
      return JAVA_DATE_FORMAT.format(new Date);
    } else {
      val date = JAVA_DATE_FORMAT.format(dateTime);
      return date;
    }
  }

  def toDString(dateTime: Date): java.lang.String = {
    if (dateTime == null) {
      return JAVA_D_FORMAT.format(new Date);
    } else {
      val date = JAVA_D_FORMAT.format(dateTime);
      return date;
    }
  }

}
