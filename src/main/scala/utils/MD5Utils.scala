package utils

/**
 * Created by Tongzhenguo on 2017/6/2.
 */
object MD5Utils extends Serializable{

  def MD5(s:String)={
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b,0,b.length)
    new java.math.BigInteger(1,m.digest()).toString(16)
  }

  def main(args: Array[String]) {
    println(MD5("1111111"))

  }

}
