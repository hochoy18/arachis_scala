package scopt

/**
  *
  * Created by tongzhenguo on 2018/6/7.
  */
import java.io.File
import java.net.UnknownHostException

private[scopt] object platform {
  val _NL = System.getProperty("line.separator")

  type ParseException = Exception
  def mkParseEx(s: String, p: Int) = new Exception(s"$s at $p")

  trait PlatformReadInstances {
    implicit val fileRead: Read[File] = Read.reads { new File(_) }
  }

  def applyArgumentExHandler[C](desc: String, arg: String): PartialFunction[Throwable, Either[Seq[String], C]] = {
    case e: NumberFormatException => Left(Seq(desc + " expects a number but was given '" + arg + "'"))
    case e: Throwable             => Left(Seq(desc + " failed when given '" + arg + "'. " + e.getMessage))
  }


}
