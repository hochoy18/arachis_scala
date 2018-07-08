package scopt

import scala.util.parsing.json.JSON

/**
  *
  * Created by tongzhenguo on 2018/6/26.
  *
  */
object OptionConfigUserCase {

  var conf: OptionConfig = _

  def main(args: Array[String]): Unit = {

    val confJson = """{"a":1,"list":[1,2,3]}"""
    println(confJson)
    // {"a":1,"list":[1,2,3]}
    val params = raw"--add;1;--printMsg;   a;--aggregate_conf;$confJson"
    run(params.split(";"))
  }

  def run(args: Array[String]): Unit = {

    menu(args)
    println( conf.aggregate_conf)
    // {"a":1,"list":[1,2,3]}
    val jsonConf = JSON.parseFull(conf.aggregate_conf).get.asInstanceOf[Map[String, Any]]
    println(jsonConf.mkString(","))
    // a -> 1.0,list -> List(1.0, 2.0, 3.0)
    println( s"add=${conf.add},printMsg=${conf.printMsg},aggregate_conf=${jsonConf.mkString(":")}" )
    // add=1,printMsg=   a,aggregate_conf=a -> 1.0:list -> List(1.0, 2.0, 3.0)
  }

  def menu(args: Array[String]): Unit = {
    println(args.mkString(" "))
    // --add 1 --printMsg    a --aggregate_conf {"a":1,"list":[1,2,3]}
    ScheduleMenu.parser.parse(args, OptionConfig()) match {
      case Some(cfg) => {
        conf = cfg.copy()
      }
      case None => {}
    }
  }

}
