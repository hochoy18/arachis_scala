package scopt

/**
  *
  * Created by tongzhenguo on 2018/6/26.
  */
case class OptionConfig(
                         add:Int = 1,
                         printMsg:String = "",
                         aggregate_conf:String = raw"{}"
                       )

object ScheduleMenu {
  val parser = new OptionParser[OptionConfig]("") {
    head("", "")

    opt[Int]("add").action((x, c) =>
      c.copy(add = x)).text("add operation")

    opt[String]("printMsg").action((x, c) =>
      c.copy(printMsg = x)).text("the print message")

    opt[String]("aggregate_conf").required().action((x, c) =>
      c.copy(aggregate_conf = x))


    help("help").text("prints this usage info and exit")
    help("version").text("displays version info and exit")
  }
}
