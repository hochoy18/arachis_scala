package learntoscala

/**
 * Created by Tongzhenguo on 2017/6/6.
 */
object MatchUserCase extends Serializable{

  def getSpecialDay( day:Int ) :String = {

    val specialDay = day match {
      case 20170606 => "护眼日"
      case 20171111 => "光棍节"
    }
    specialDay
  }

  def validParamType(args:Any): Unit ={

    args match {

      case _:String => println(args)
      case exceptVal => throw new Exception("not excepted value:"+exceptVal)
    }
  }

  def main(args: Array[String]) {
      println( getSpecialDay(20171111) )
      // 光棍节
      validParamType(null)
      /*
 Exception in thread "main" java.lang.Exception: not excepted value:null
	at learntoscala.MatchUserCase$.validParamType(MatchUserCase.scala:22)
	at learntoscala.MatchUserCase$.main(MatchUserCase.scala:29)
	at learntoscala.MatchUserCase.main(MatchUserCase.scala)
      */

  }


}
