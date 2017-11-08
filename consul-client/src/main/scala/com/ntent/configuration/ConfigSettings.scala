package com.ntent.configuration

import rx.lang.scala.Observable
import scala.reflect.runtime.universe._

trait ConfigSettings {
  private lazy val _hostFQDN = java.net.InetAddress.getLocalHost.getHostName

  def get(key: String): String = get(key, useDefaultKeystores = true).getOrElse(throw new RuntimeException(s"Key not found '$key'")).value

  def getAs[T : _root_.scala.reflect.runtime.universe.TypeTag](key:String): T = {
    convert[T](get(key))
  }

  def getList[T: _root_.scala.reflect.runtime.universe.TypeTag](key: String, delimiter: String = ","): List[T] = {
    val value = get(key)
    (for{
      v <- value.split(delimiter)
      cv = convert[T](v)
    } yield cv).toList
  }

  def convert[T : _root_.scala.reflect.runtime.universe.TypeTag](value:String):T = {
    typeOf[T] match {
      case t if t =:= typeOf[String] => value.asInstanceOf[T]
      case t if t =:= typeOf[Int] => value.toInt.asInstanceOf[T]
      case t if t =:= typeOf[Long] => value.toLong.asInstanceOf[T]
      case t if t =:= typeOf[Double] => value.toDouble.asInstanceOf[T]
      case t if t =:= typeOf[Boolean] => value.toBoolean.asInstanceOf[T] //doesn't work with 0/1
      case t if t =:= typeOf[Byte] => value.toByte.asInstanceOf[T]
      case _ => throw new IllegalArgumentException("Cannot convert to type " + typeOf[T].toString)
    }
  }

  /**
    * Return milliseconds from a time value like: 250ms, 90s, 5m, or 1h.
    */
  def getMs(key: String): Int = {
    getTime(key) match {
      // Assume value was in milliseconds.
      case Left(x) => x
      // Return milliseconds.
      case Right(x) => x
    }
  }

  /**
    * Return seconds from a time value like: 250ms, 90s, 5m, or 1h.
    */
  def getSec(key: String): Int = {
    getTime(key) match {
      // Assume value was in seconds.
      case Left(x) => x
      // Convert to seconds.  Round any fraction up to the next second.
      case Right(x) => (x / 1000) + (if ((x % 1000) > 0) {1} else {0})
    }
  }

  /**
    * Left(number) if no units.  Right(milliseconds) for time values like: 250ms, 90s, 5m, or 1h.
    */
  private def getTime(key: String): Either[Int, Int] = {
    // Get the value.
    val value = get(key)
    val len = value.length

    var cause: Exception = null
    try {
      // Get the last two characters of the value.
      val lastChar = value.charAt(len - 1)
      val nextToLastChar = if (len > 1) { value.charAt(len - 2) } else { '0' }

      // Parse and return, if we can identify the units.
      (nextToLastChar, lastChar) match {
        case (_, z) if z.isDigit => return Left(value.toInt)  // We can drop support for "no units", once all time values have units.
        case ('m', 's') => return Right(value.substring(0, len - 2).toInt)
        case (_, 's') => return Right(value.substring(0, len - 1).toInt * 1000)
        case (_, 'm') => return Right(value.substring(0, len - 1).toInt * 60 * 1000)
        case (_, 'h') => return Right(value.substring(0, len - 1).toInt * 60 * 60 * 1000)
        case (_, 'd') => return Right(value.substring(0, len - 1).toInt * 24 * 60 * 60 * 1000)
        case (_, _) =>
      }
    } catch {
      case ex: Exception => cause = ex
    }

    // Throw failed value and cause (if any).
    throw new IllegalStateException(s"Unable to parse time value($value) fetched from key($key)", cause)
  }

  def get(key: String, useDefaultKeystores: Boolean, namespaces: String*): Option[KeyValuePair]

  def getKeyValuePairsAt(namespace: String): Set[KeyValuePair]

  def getChildContainers: Set[String]

  def getChildContainersAt(namespace: String): Set[String]

  def liveUpdateAll(): Observable[KeyValuePair]

  def liveUpdateFolder(folder: String): Observable[KeyValuePair]

  def liveUpdateEffectiveSettings(): Observable[KeyValuePair]

  def liveUpdate(key: String, namespaces: String*): Observable[KeyValuePair]

  def liveUpdate(key: String, useDefaultKeystores: Boolean, namespaces: String*): Observable[KeyValuePair]

  protected def expandAndReverseNamespaces(nameSpaces: Array[String]): Array[String] = {
    nameSpaces.map(_.trim).reverse.map(_.replaceAllLiterally("{host}", _hostFQDN))
  }
}
