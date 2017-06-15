package utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.StringUtils

/**
 * Created by Tongzhenguo on 2017/5/27.
 */
object GzipKit extends Serializable{
  var DEFAULT_CHARSET: String = "UTF-8"

  /** *
    * 压缩字符串
    * @param source 压缩原始字符串
    * @return 返回压缩之后的字符串
    * @throws IOException
    */
  @throws(classOf[IOException])
  def compressUTF8(source: String): String = {
    return compress(source, DEFAULT_CHARSET)
  }

  /** *
    * 字符串压缩
    * @param source 原始字符串
    * @param  sourceCharset 原始字符串的编码字符集
    * @return 返回压缩的字符串
    * @throws IOException
    */
  @throws(classOf[IOException])
  def compress(source: String, sourceCharset: String): String = {
    if (StringUtils.isEmpty(source)) return source
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    val gzip: GZIPOutputStream = new GZIPOutputStream(out)
    gzip.write(source.getBytes(sourceCharset))
    gzip.close
    return Base64.encodeBase64String(out.toByteArray)
  }

  /** *
    * 字符串解压
    * @param src 原始字符串
    * @param charset 目标字符串编码字符集
    * @return 返回解压的字符串
    * @throws IOException
    */
  @throws(classOf[IOException])
  def uncompress(src: String, charset: String): String = {
    if (StringUtils.isBlank(src)) return src
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    val in: ByteArrayInputStream = new ByteArrayInputStream(Base64.decodeBase64(src))
    val gzipInputStream: GZIPInputStream = new GZIPInputStream(in)
    val buffer: Array[Byte] = new Array[Byte](256)
    var n: Int = 0
    while ( {
      n = gzipInputStream.read(buffer);
      n
    } >= 0) {
      out.write(buffer, 0, n)
    }
    return out.toString(charset)
  }

  /** *
    * 解压字符串
    * @param src 原始字符串
    * @return 解压的字符串
    * @throws IOException
    */
  @throws(classOf[IOException])
  def uncompressUTF8(src: String): String = {
    return uncompress(src, DEFAULT_CHARSET)
  }

}
