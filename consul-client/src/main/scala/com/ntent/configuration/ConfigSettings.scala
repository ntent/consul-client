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
