package com.ntent.configuration

import rx.lang.scala.Observable
import scala.reflect.runtime.universe._

trait ConfigSettings {
  private lazy val _hostFQDN = java.net.InetAddress.getLocalHost.getHostName

  def keystores : Seq[String]

  /** Get config value for this key.  Throw if not found. */
  def get(key: String): String = get(key, useDefaultKeystores = true).getOrElse(throw new RuntimeException(s"Key not found '$key'")).value

  /** Get config value for this key, converted to specified type.  Throw if not found, wrong type, or unsupported type. */
  def getAs[T : _root_.scala.reflect.runtime.universe.TypeTag](key:String): T = convert[T](get(key))

  /** Return milliseconds for a time value like: 250ms, 90s, 5m, or 1h.  (Uppercase also fine.) */
  def getMs(key: String): Long = getTime(key, 1)
  /** Return seconds for a time value like: 250ms, 90s, 5m, or 1h.  Fractions like "10ms" round up to 1 second, not zero. */
  def getSec(key: String): Long = getTime(key, 1000)
  /** Return minutes for a time value like: 250ms, 90s, 5m, or 1h.  Fractions like "10ms" round up to 1 minute, not zero. */
  def getMinutes(key: String): Long = getTime(key, 60 * 1000)

  /** Returns bytes for a size value like: 500b, 100k, 100kb, 30m, 30mb, 2g, or 2gb.  (Uppercase also fine.) */
  def getBytes(key: String): Long = getSize(key, 1)
  /** Returns kilobytes from a size value like: 250b, 100k, 30m, or 2g.  Fractions like "100b" round up to 1 KB, not zero. */
  def getKb(key: String): Long = getSize(key, 1024)
  /** Returns megabytes from a size value like: 250b, 100k, 30m, or 2g.  Fractions like "100b" round up to 1 MB, not zero. */
  def getMb(key: String): Long = getSize(key, 1024 * 1024)
  /** Returns gigabytes from a size value like: 250b, 100k, 30m, or 2g.  Fractions like "10b" round up to 1 GB, not zero. */
  def getGb(key: String): Long = getSize(key, 1024 * 1024 * 1024)

  /** Split a value on specified delimiter, return that list of values. */
  def getList[T: _root_.scala.reflect.runtime.universe.TypeTag](key: String, delimiter: String = ","): List[T] = {
    val value = get(key)
    (for{
      v <- value.split(delimiter)
      cv = convert[T](v)
    } yield cv).toList
  }

  /** Convert value to specified type T.  Throw if value cannot be converted to this type, or unsupported type. */
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

  def get(key: String, useDefaultKeystores: Boolean, namespaces: String*): Option[KeyValuePair]

  def getKeyValuePairsAt(namespace: String): Set[KeyValuePair]

  def getChildContainers: Set[String]

  def getChildContainersAt(namespace: String): Set[String]

  /**
    * Get the current list of "effective settings" (just the settings actively in use based on list of configStores)
    * @return sequence of effective setting KeyValuePair objects. The Key should be the full path for the setting, not just its name.
    */
  def getEffectiveSettings : Seq[KeyValuePair]

  def liveUpdateAll(): Observable[KeyValuePair]

  def liveUpdateFolder(folder: String): Observable[KeyValuePair]

  def liveUpdateEffectiveSettings(): Observable[KeyValuePair]

  def liveUpdate(key: String, namespaces: String*): Observable[KeyValuePair]

  def liveUpdate(key: String, useDefaultKeystores: Boolean, namespaces: String*): Observable[KeyValuePair]

  protected def expandAndReverseNamespaces(nameSpaces: Array[String]): Array[String] = {
    nameSpaces.map(_.trim).reverse.map(_.replaceAllLiterally("{host}", _hostFQDN))
  }

  /**
    * Converts a time value like: 250ms, 90s, 5m, or 1h to bytes, then divides by specified divisor (rounding up all fractions).
    * If no units, return value, ignoring divisor.  (Assume the value is already in that unit.)
    */
  private def getTime(key: String, divisor: Long): Long = {
    // Get the value.
    val value = get(key)

    try {
      // Calculate desired units, from this value.
      val (digits, suffix) = splitDigitsFromSuffix(value)
      suffix match {
        case "ms" => divCeil(digits, divisor)
        case "s"  => divCeil(digits * 1000, divisor)
        case "m"  => divCeil(digits * 60 * 1000, divisor)
        case "h"  => divCeil(digits * 60 * 60 * 1000, divisor)
        case "d"  => divCeil(digits * 24 * 60 * 60 * 1000, divisor)
        case _ =>
          throw new IllegalStateException("Units unrecognized.")
      }
    } catch {
      case ex: Exception => throw new IllegalStateException(s"Unable to parse time value($value) fetched from key($key)", ex)
    }
  }

  /**
    * Converts a size value like: 500b, 100k, 30m, or 2g to bytes, then divides by specified divisor (rounding up all fractions).
    * If no units, return value, ignoring divisor.  (Assume the value is already in that unit.)
    */
  private def getSize(key: String, divisor: Long): Long = {
    // Get the value.
    val value = get(key)

    try {
      // Calculate desired units, from this value.
      val (digits, suffix) = splitDigitsFromSuffix(value)
      suffix match {
        case "b"        => divCeil(digits, divisor)
        case "k" | "kb" => divCeil(digits * 1024, divisor)
        case "m" | "mb" => divCeil(digits * 1024 * 1024, divisor)
        case "g" | "gb" => divCeil(digits * 1024 * 1024 * 1024, divisor)
        case "" =>
          // Assume value is already in the "units" requested by caller.
          // We can drop support for "no units", once all Consul time values have units.
          digits
        case _ =>
          throw new IllegalStateException("Units unrecognized.")
      }
    } catch {
      case ex: Exception => throw new IllegalStateException(s"Unable to parse size value($value) fetched from key($key)", ex)
    }
  }

  private def splitDigitsFromSuffix(value: String): (Long, String) = {
    val suffixPos = value.indexWhere(c => !c.isDigit) match {
      case -1 => value.length
      case x => x
    }
    val digits = value.substring(0, suffixPos).toLong
    val suffix = value.substring(suffixPos).toLowerCase
    (digits, suffix)
  }

  /**
    * Divide x by divisor.  Round all fractions up.
    */
  private def divCeil(x: Long, divisor: Long): Long = {
    if (divisor <= 1) {
      x
    } else {
      (x / divisor) + (if ((x % divisor) > 0) {1} else {0})
    }
  }

}
