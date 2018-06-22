package SClearn

import scala.io.BufferedSource


/**
  *
  * Created by tongzhenguo on 2018/6/22.
  * scala 实现文件系统操作用例
  */
object FSopUserCase {

  def main(args: Array[String]): Unit = {
    // 写文件
    import java.io._
    val writer = new PrintWriter(new File("test.txt" ))
    writer.write("Arachis Scala Fs write test\nscala 实现文件系统操作用例")
    writer.close()

    // 读文件
    import scala.io.Source
    val bs: BufferedSource = Source.fromFile("test.txt",enc = "utf-8" )
    val lines = bs.getLines()
    for(line <- lines ){
      println(line)
    }

    // 创建目录
    var dir = "data"
    val bool = new File(s"./$dir").mkdirs()
    val signal = if(bool) "success" else "fail"
    println(raw"create dir $dir ${if(bool) "success" else "fail"}")

    // 查看目录
    dir = "src/main/scala/SClearn"
    val filePaths = new File(s"./$dir").list()
    val files = new File(s"./$dir").listFiles()
    println(filePaths.mkString(","))
    // ListLearn.scala,closure.scala,ImplicitTransferLearn.scala,MapUserCase.scala,MatchUserCase.scala...
    println(files.mkString(","))
    // ./src/main/scala/SClearn/ListLearn.scala,./src/main/scala/SClearn/closure.scala...

    // 判断文件存在
    val file = "FSopUserCase.scala"
    val exist = new File(s"./$dir/$file").exists()
    println(raw"file $file ${if(exist) "exists" else "not exists"}")
    // file FSopUserCase.scala exists

    // 删除文件或者目录
    var isDelete = new File("test.txt").delete()
    println(raw"file test.txt delete ${if(isDelete) "success" else "fail"}" )
    // file test.txt delete success
    dir = "data"
    isDelete = new File(s"$dir").delete()
    println(raw"dir $dir delete ${if(isDelete) "success" else "fail"}" )
    // dir data delete success

  }

}
