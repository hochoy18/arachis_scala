package learntoscala

/**
 * Created by Tongzhenguo on 2017/6/6.
 */
object MatchValueAndType extends Serializable{

  def getSpecialDay( day:Int ) :String = {

    val specialDay = day match {
      case 20170606 => "护眼日"
      case 20171111 => "光棍节"
    }
    specialDay
  }

  def main(args: Array[String]) {
      println( getSpecialDay(20171111) )



  }


}
