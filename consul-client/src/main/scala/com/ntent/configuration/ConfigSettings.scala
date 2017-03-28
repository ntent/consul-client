package com.ntent.configuration

import rx.lang.scala.Observable

trait ConfigSettings {

  def get(key: String): String

  def getAs[T: _root_.scala.reflect.runtime.universe.TypeTag](key: String): T

  def getList[T: _root_.scala.reflect.runtime.universe.TypeTag](key: String, delimiter: String = ","): List[T]

  def convert[T: _root_.scala.reflect.runtime.universe.TypeTag](value: String): T

  def get(key: String, useDefaultKeystores: Boolean, namespaces: String*): Option[(String, String)]

  def getChildContainers(): Set[String]

  def getChildContainersAt(namespace: String): Set[String]

  def liveUpdateAll(): Observable[(String,String)]

  def liveUpdateFolder(folder: String): Observable[(String,String)]

  def liveUpdate(key: String, namespaces: String*): Observable[(String,String)]

  def liveUpdate(key: String, useDefaultKeystores: Boolean, namespaces: String*): Observable[(String,String)]

}
