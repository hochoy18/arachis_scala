package scopt

/**
  *
  * Created by tongzhenguo on 2018/6/26.
  *
  */
object OptionConfigUserCase {

  var conf: OptionConfig = _

  def main(args: Array[String]): Unit = {

    val params = raw"--add;1;--printMsg; "
    run(params.split(";"))
  }

  def run(args: Array[String]): Unit = {

    menu(args)
    println( s"add=${conf.add},printMsg=${conf.printMsg}" )

  }

  def menu(args: Array[String]): Unit = {

    ScheduleMenu.parser.parse(args, OptionConfig()) match {
      case Some(cfg) => {
        conf = cfg.copy()
      }
      case None => {}
    }
  }

}
