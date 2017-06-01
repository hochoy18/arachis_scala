package utils

import java.security.SecureRandom
import javax.crypto.spec.DESedeKeySpec
import javax.crypto.{Cipher, SecretKey, SecretKeyFactory}

/**
 * Created by Tongzhenguo on 2017/5/27.
 */
object DESedeCodec {
  private val ALGORITHM: String = "DESede"
  private val RAW_KEY: Array[Byte] = "PiadX_d(a+;@#!@3A^&EE>OP".getBytes//秘钥
  private val SECURE_RANDOM: SecureRandom = new SecureRandom;

  /**
   * 转换密钥
   */
  @throws(classOf[Exception])
  private def toKey(key: Array[Byte]): SecretKey = {
    val dks: DESedeKeySpec = new DESedeKeySpec(key)
    val keyFactory: SecretKeyFactory = SecretKeyFactory.getInstance(ALGORITHM)
    val secretKey: SecretKey = keyFactory.generateSecret(dks)
    return secretKey
  }

  /** *
    * 加密数据
    * @param data 待加密数据
    * @return 加密后的数据
    */
  @throws(classOf[Exception])
  def encrypt(data: String): String = {
    val key: SecretKey = toKey(RAW_KEY)
    val cipher: Cipher = Cipher.getInstance(ALGORITHM)
    cipher.init(Cipher.ENCRYPT_MODE, key, SECURE_RANDOM)
    return HexUtils.byte2hex(cipher.doFinal(data.getBytes))
  }

  /** *
    * 解密数据
    * @param hexData 待解密数据
    * @return 解密后的数据
    */
  @throws(classOf[Exception])
  def decrypt(hexData: String): String = {
    val key: SecretKey = toKey(RAW_KEY)
    val cipher: Cipher = Cipher.getInstance(ALGORITHM)
    cipher.init(Cipher.DECRYPT_MODE, key, SECURE_RANDOM)
    return new String(cipher.doFinal(HexUtils.hex2byte(hexData)))
  }

}
