package com.ntent.configuration

import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigUtil, ConfigValueType}
import com.typesafe.scalalogging.slf4j.StrictLogging
import rx.lang.scala.Observable

import scala.collection.JavaConverters._

/**
  * ConfigSettings implementation based on Typesafe Config. See https://github.com/typesafehub/config
  */
class TypesafeConfigSettings extends ConfigSettings with StrictLogging {
  private val config = ConfigFactory.load()
  // look for and set the "default keystores
  private val defaultKeyStores: Array[String] =
    if (config.hasPath("dconfig.consul.keyStores"))
      expandAndReverseNamespaces(config.getString("dconfig.consul.keyStores").split(" |,|\\|"))
    else
      Array[String]()


  override def get(key: String, useDefaultKeystores: Boolean, namespaces: String*): Option[KeyValuePair] = {
    // look in each namespace in turn, then in the root.
    val allPaths = if(useDefaultKeystores) namespaces.reverse ++ defaultKeyStores else namespaces.reverse
    (for {
      ns <- allPaths
      path = if (ns == null || ns.isEmpty) key else ConfigUtil.joinPath((Seq(ns) ++ ConfigUtil.splitPath(key).asScala).asJava)
      if config.hasPath(path)
    } yield KeyValuePair(path, config.getString(path))).headOption
  }

  override def getKeyValuePairsAt(namespace: String): Set[KeyValuePair] = {
    if (!config.hasPath(namespace))
      Set[KeyValuePair]()
    else {
      val subConfig = config.getConfig(namespace)
      subConfig.entrySet().asScala.map(e=>KeyValuePair(e.getKey,e.getValue.render())).toSet
    }
  }

  override def getChildContainers: Set[String] = {
    getChildContainers("")
  }

  override def getChildContainersAt(namespace: String): Set[String] = {
    getChildContainers(namespace)
  }

  private def getChildContainers(path: String): Set[String] = {
    // get the ConfigValue at the requested path or the root.
    val pathValue = if (path == null || path == "") config.root() else config.getValue(path)
    // the value at that path needs to be an object (container) otherwise there are no children
    if (pathValue.valueType() != ConfigValueType.OBJECT)
      Set[String]()
    else {
      val containers = pathValue.asInstanceOf[ConfigObject].
        entrySet().asScala.
        filter(e=>e.getValue.valueType() == ConfigValueType.OBJECT).
        map(e=>e.getKey)
      containers.toSet
    }
  }

  // Live Update functions not implmemented. Config is static from Properties files.
  // TODO: Implement a fixed observable?
  override def liveUpdateAll(): Observable[KeyValuePair] = { Observable.empty }
  override def liveUpdateFolder(folder: String): Observable[KeyValuePair] = { Observable.empty }
  override def liveUpdateEffectiveSettings(): Observable[KeyValuePair] = { Observable.empty }
  override def liveUpdate(key: String, namespaces: String*): Observable[KeyValuePair] = { Observable.empty }
  override def liveUpdate(key: String, useDefaultKeystores: Boolean, namespaces: String*): Observable[KeyValuePair] = { Observable.empty }
}
