package com.ntent.configuration

import rx.lang.scala.Observable

trait ConfigSettings {

  /** Return (value, namespace) */
  def getWithNamespace(key: String): Option[(String, String)]

  def get(key: String): String

  def getAs[T: _root_.scala.reflect.runtime.universe.TypeTag](key: String): T

  def getList[T: _root_.scala.reflect.runtime.universe.TypeTag](key: String, delimiter: String = ","): List[T]

  def convert[T: _root_.scala.reflect.runtime.universe.TypeTag](value: String): T

  def get(key: String, useDefaultKeystores: Boolean, namespaces: String*): Option[(String, String)]

  def getChildContainers(): Set[String]

  def getChildContainersAt(namespace: String): Set[String]

  def liveUpdate(): Observable[String]

  def liveUpdate(key: String, namespaces: String*): Observable[String]
}
