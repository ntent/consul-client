package com.ntent.configuration

import com.typesafe.config._
import com.typesafe.scalalogging.StrictLogging
import rx.lang.scala.Observable

import scala.collection.JavaConverters._

/**
  * ConfigSettings implementation based on Typesafe Config. See https://github.com/typesafehub/config
  */
class TypesafeConfigSettings extends ConfigSettings with StrictLogging {
  private val config = ConfigFactory.load()
  // look for and set the "default keystores"
  private val defaultKeyStores: Array[String] =
    if (config.hasPath("dconfig.consul.keyStores"))
      expandAndReverseNamespaces(config.getString("dconfig.consul.keyStores").split(" |,|\\|"))
    else
      Array[String]()

  // look for and set the "configRootPath"
  private val configRootPath: String =
    if (config.hasPath("config.rootpath")) { config.getString("config.rootpath").stripSuffix(".") + "." } else ""

  override def keystores: Seq[String] = defaultKeyStores.reverse

  override def get(key: String, useDefaultKeystores: Boolean, namespaces: String*): Option[KeyValuePair] = {
    // look in each namespace in turn, then in the root.
    val allPaths = if(useDefaultKeystores) namespaces.reverse ++ defaultKeyStores else namespaces.reverse
    (for {
      ns <- allPaths
      path = if (ns == null || ns.isEmpty) key
        else ConfigUtil.joinPath((splitPath(ns) ++ splitPath(key)).asJava)
      fullPath = configRootPath + path
      if config.hasPath(fullPath)
    } yield KeyValuePair(fullPath, config.getString(fullPath), key)).headOption
  }

  override def getKeyValuePairsAt(namespace: String): Set[KeyValuePair] = {
    if (!config.hasPath(namespace))
      Set[KeyValuePair]()
    else {
      val subConfig = config.getConfig(configRootPath + namespace)
      val configRender = ConfigRenderOptions.concise().setFormatted(false).setJson(false).setComments(false)
      subConfig.entrySet().asScala.map(e=>KeyValuePair(configRootPath + (if (namespace != null && namespace.length > 0) namespace + "." else "") + e.getKey,e.getValue.render(configRender),e.getKey)).toSet
    }
  }

  override def getChildContainers: Set[String] = {
    getChildContainers(configRootPath)
  }

  override def getChildContainersAt(namespace: String): Set[String] = {
    getChildContainers(configRootPath + namespace)
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

  private def splitPath(path: String): List[String] = {
    val forward = path.indexOf('/')
    val backward = path.indexOf('\\')
    if (path.isEmpty)
      List("")
    else if (forward == -1 && backward == -1)
      ConfigUtil.splitPath(path).asScala.toList
    else {
      val sep = if (forward != -1) '/' else '\\'
      path.split(sep).toList
    }
  }

  override def getEffectiveSettings: Seq[KeyValuePair] = {
    val allEntries = config.entrySet()
    val res = for {
      entry <- allEntries.asScala
      kv = KeyValuePair(configRootPath + entry.getKey,entry.getValue.render(),entry.getKey)
      value = get(kv.fullPath,useDefaultKeystores = true)
      if value.isDefined
    } yield value.get
    res.toSeq.distinct
  }

  override def getAllSettings: Seq[KeyValuePair] = {
    val allEntries = config.entrySet()
    val res = for {
      entry <- allEntries.asScala
      kv = KeyValuePair(configRootPath + entry.getKey,entry.getValue.render(),entry.getKey)
    } yield kv
    res.toSeq
  }

  // Live Update functions not implmemented. Config is static from Properties files.
  // TODO: Implement a fixed observable?
  override def liveUpdateAll(): Observable[KeyValuePair] = { Observable.empty }
  override def liveUpdateFolder(folder: String): Observable[KeyValuePair] = { Observable.empty }
  override def liveUpdateEffectiveSettings(): Observable[KeyValuePair] = { Observable.empty }
  override def liveUpdate(key: String, namespaces: String*): Observable[KeyValuePair] = { Observable.empty }
  override def liveUpdate(key: String, useDefaultKeystores: Boolean, namespaces: String*): Observable[KeyValuePair] = { Observable.empty }
}
