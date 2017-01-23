package hdfs

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.mapred.JobConf

/**
 * Created by arachis on 2016/12/30.
 * the scala version of hdfs operation
 */
object HDFSUtils extends Serializable {

  private var conf: Configuration = new JobConf()
  val HDFS = "hdfs://namenode_host:8020"


  @throws(classOf[IOException])
  def ls(folder: String) {
    val path: Path = new Path(folder)
    val fs: FileSystem = FileSystem.get(URI.create(HDFS), conf)
    val list: Array[FileStatus] = fs.listStatus(path)
    println("ls: " + folder)
    println("==========================================================")
    for (f <- list) {
      println("name: %s, folder: %s, size: %d\n", f.getPath, f.isDir, f.getLen)
    }
    println("==========================================================")
    fs.close
  }

  @throws(classOf[IOException])
  def mkdirs(folder: String) {
    val path: Path = new Path(folder)
    val fs: FileSystem = FileSystem.get(URI.create(HDFS), conf)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
      println("Create: " + folder)
    }
    fs.close
  }

  @throws(classOf[IOException])
  def rmr(folder: String) {
    val path: Path = new Path(folder)
    val fs: FileSystem = FileSystem.get(URI.create(HDFS), conf)
    fs.deleteOnExit(path)
    println("Delete: " + folder)
    fs.close
  }

  @throws(classOf[IOException])
  def exists(folder: String): Boolean = {
    var isExists: Boolean = false
    val path: Path = new Path(folder)
    val fs: FileSystem = FileSystem.get(URI.create(HDFS), conf)
    isExists = fs.exists(path)
    println("isExists: " + isExists + "  " + folder)
    fs.close
    return isExists
  }

  @throws(classOf[IOException])
  def rmrIfExist(folder: String) {
    var flag: Boolean = false
    val path: Path = new Path(folder)
    val fs: FileSystem = FileSystem.get(URI.create(HDFS), conf)
    if (fs.exists(path)) {
      flag = fs.deleteOnExit(path)
    }
    println("Deleted: " + flag + "_" + folder)
    fs.close
  }

  @throws(classOf[IOException])
  def saveAsFile(file: String, content: String) {
    val fs: FileSystem = FileSystem.get(URI.create(HDFS), conf)
    val buff: Array[Byte] = content.getBytes
    var os: FSDataOutputStream = null
    try {
      os = fs.create(new Path(file))
      os.write(buff, 0, buff.length)
      println("saveAsFile: " + file)
    } finally {
      if (os != null) {
        os.close
      }
    }
    fs.close
  }

  @throws(classOf[IOException])
  def copyFile(local: String, remote: String) {
    val fs: FileSystem = FileSystem.get(URI.create(HDFS), conf)
    fs.copyFromLocalFile(new Path(local), new Path(remote))
    println("copy from: " + local + " to " + remote)
    fs.close
  }

  @throws(classOf[IOException])
  def download(remote: String, local: String) {
    val path: Path = new Path(remote)
    val fs: FileSystem = FileSystem.get(URI.create(HDFS), conf)
    fs.copyToLocalFile(path, new Path(local))
    println("download: from" + remote + " to " + local)
    fs.close
  }

}
