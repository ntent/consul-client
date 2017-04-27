package com.ntent.configuration

import rx.lang.scala.Observable

trait ConfigSettings {

  def get(key: String): String

  def getAs[T: _root_.scala.reflect.runtime.universe.TypeTag](key: String): T

  def getList[T: _root_.scala.reflect.runtime.universe.TypeTag](key: String, delimiter: String = ","): List[T]

  def convert[T: _root_.scala.reflect.runtime.universe.TypeTag](value: String): T

  def get(key: String, useDefaultKeystores: Boolean, namespaces: String*): Option[KeyValuePair]

  def getKeyValuePairsAt(namespace: String): Set[KeyValuePair]

  def getChildContainers: Set[String]

  def getChildContainersAt(namespace: String): Set[String]

  def liveUpdateAll(): Observable[KeyValuePair]

  def liveUpdateFolder(folder: String): Observable[KeyValuePair]

  def liveUpdate(key: String, namespaces: String*): Observable[KeyValuePair]

  def liveUpdate(key: String, useDefaultKeystores: Boolean, namespaces: String*): Observable[KeyValuePair]

}
