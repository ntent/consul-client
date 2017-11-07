package com.ntent.configuration

import com.typesafe.scalalogging.slf4j.StrictLogging
import rx.lang.scala.{Observable, Subject}
import rx.lang.scala.schedulers.ExecutionContextScheduler
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.annotation.JsonProperty
import com.typesafe.config.ConfigFactory
import org.apache.commons.codec.binary.Base64

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise, blocking}
import scala.util.Success

/**
  * Created by vchekan on 2/3/2016.
  */
class Dconfig(rootPath: String, defKeyStores: String*) extends StrictLogging with ConfigSettings {
  private val appSettings = ConfigFactory.load()
  //Make sure configRoot path doesn't have a leading '/' and ends with a single '/'
  lazy val configRootPath: String = rootPath.stripMargin('/').stripSuffix("/") + '/'
  val consulApi: ConsulApiImplDefault = new ConsulApiImplDefault()
  private var defaultKeyStores = defKeyStores
  if(defKeyStores.nonEmpty) {
    defaultKeyStores = expandAndReverseNamespaces(defKeyStores.toArray)
  } else {
    defaultKeyStores = expandAndReverseNamespaces(appSettings.getString("dconfig.consul.keyStores").split(" |,|\\|"))
  }
  val keystores: Seq[String] = defaultKeyStores.reverse

  private val events = Subject[KeyValuePair]()
  private lazy val distictChanges = events.groupBy(kv=>kv.key).flatMap(kv=>kv._2.distinctUntilChanged)
  private var settings:Map[String,String] = _
  private val readingLoopTask = Promise[Boolean]

  {

    // initialize a print-out of changes. this subscription doesn't print until there are two entries (a new value changed)
    // except it does flush when the subscription ends (even if the value never changed)
    events.groupBy(kv=>kv.key).flatMap(kv=>kv._2.distinctUntilChanged.slidingBuffer(2,1)).subscribe(kvs => {
      if (kvs.length == 2)
        logger.info(s"Changed config: ${kvs.head.key} : ${kvs.head.value} => ${kvs(1).value}")
    }, e=> logger.error("Error in config change subscription. No longer printing configuration updates.",e))

    initialRead()

    startReadingLoop(readingLoopTask.future)

  }

  def this() {
    this(ConfigFactory.load().getString("dconfig.consul.configRoot").stripMargin('/') + '/')
  }

  private[configuration] def close() = {
    events.onCompleted()
    readingLoopTask.complete(Success(true))
  }

  private def ensureOpen(): Unit = {
    if (readingLoopTask.isCompleted)
      throw new Exception("This Dconfig instance is closed. It cannot be used anymore.")
  }

  override def get(key: String, useDefaultKeystores: Boolean, namespaces: String*): Option[KeyValuePair] = {
    ensureOpen()
    val allNamespaces = if(useDefaultKeystores) namespaces.reverse ++ defaultKeyStores else namespaces.reverse
    (for {
      ns <- allNamespaces
      path = "/" + configRootPath + ns + "/" + key
      s = settings.get(path)
      if s.isDefined
    } yield KeyValuePair(ns + "/" + key, s.get)).headOption
  }

  override def getKeyValuePairsAt(namespace: String): Set[KeyValuePair] = {
    ensureOpen()
    val path = "/" + configRootPath + namespace
    settings.filter(p => p._1.startsWith(path) && !p._1.endsWith("/")).map(f => {
      val key = f._1.substring(f._1.lastIndexOf("/") + 1)
      KeyValuePair(key, f._2)
    }).toSet
  }

  override def getChildContainers: Set[String] = {
    val path = "/" + configRootPath
    getChildContainers(path)
  }

  override def getChildContainersAt(namespace: String): Set[String] = {
    val path = "/" + configRootPath + namespace + "/"
    getChildContainers(path)
  }

  private def getChildContainers(path: String): Set[String] = {
    ensureOpen()
    for {
      key <- settings.keySet
      if key.startsWith(path) && getContainerName(key, path).isDefined
      container <- getContainerName(key, path)
    } yield container
  }

  private def getContainerName(key: String, rootPath: String): Option[String] = {
    if(key.indexOf("/", rootPath.length + 1) > 0)
      Some(key.substring(rootPath.length, key.indexOf("/", rootPath.length + 1)))
    else None
  }

  override def liveUpdateAll(): Observable[KeyValuePair] = {
    liveUpdateFolder("")
  }

  override def liveUpdateFolder(folder: String): Observable[KeyValuePair] = {
    var folderWithRoot = s"/$configRootPath$folder"
    if (!folderWithRoot.endsWith("/")) folderWithRoot += "/"

    ensureOpen()
    val uniqueChanges = distictChanges
    // only changes that are a sub-path of the given folder.
    uniqueChanges.withFilter(kv => kv.key.startsWith(folderWithRoot))
      .map(kv=>kv)
  }

  override def liveUpdateEffectiveSettings() : Observable[KeyValuePair] = {
    ensureOpen()

    // distinct will only alert us of changes, but we want to emit all values when first subscribed to.
    // concatenate the existing keys/values with the upcoming changes.
    val ob = Observable.from(settings).map(t=>KeyValuePair(t._1,t._2)) ++ distictChanges

    // get just the key of the setting names (removing the path data)
    val res:Observable[KeyValuePair] = for {
      kv <- ob
      settingName = extractKeyFromPath(kv.key, defaultKeyStores.toList)
      value = get(settingName, useDefaultKeystores = true)
      if value.isDefined
    } yield KeyValuePair(settingName, value.get.value)

    res.groupBy(kv=>kv.key).flatMap(kv=>kv._2.distinctUntilChanged)
  }

  private def extractKeyFromPath(path:String, namespaces:List[String]):String = {
    namespaces match {
      case s :: rest =>
        val root = "/" + configRootPath + s + "/"
        if (path.startsWith(root))
          path.substring(root.length)
        else
          extractKeyFromPath(path,rest)
      case Nil => path
    }
  }

  override def liveUpdate(key: String, namespaces: String*): Observable[KeyValuePair] = {
    liveUpdate(key,true, namespaces:_*)
  }

  override def liveUpdate(key: String, useDefaultKeystores: Boolean, namespaces: String*): Observable[KeyValuePair] = {
    ensureOpen()
    val keyStores = if (useDefaultKeystores) namespaces.reverse ++ defaultKeyStores else namespaces.reverse
    val trackingPaths: Set[String] = (for (ns <- keyStores)
      yield "/" + configRootPath + ns + "/" + key
      ).toSet

    logger.info(s"listening to paths: $trackingPaths")

    // group by key and perform a distinctUntilChanged on each observable within the key
    val uniqueChanges = distictChanges

    uniqueChanges.withFilter(kv => {
      val res = trackingPaths.contains(kv.key)
      if(res)
        logger.debug(s"Tracking paths contains '${kv.key}' with value '${kv.value}'.")
      res
    }).
      map(_=> get(key,useDefaultKeystores,namespaces:_*)).
      withFilter(_.isDefined).
      map(_.get).
      distinctUntilChanged.
      doOnNext(v => logger.info(s"Watched config updated: ${v.key} : ${v.value}")).
      //observeOn(TrampolineScheduler())
      observeOn(ExecutionContextScheduler(scala.concurrent.ExecutionContext.global))
  }

  private def initialRead(): Unit = {
    val keys = consulApi.read(configRootPath)
    rebuild(keys)
    logger.info(s"Default Keystores are (${defaultKeyStores.mkString(",")})")
    settings.foreach(kv=>logger.info(s"Loaded config: ${kv._1} : ${kv._2}"))

  }

  private def startReadingLoop(cancellation: Future[Boolean]) = {
    scala.concurrent.Future {
      blocking {
        var done = false
        while(!done && !cancellation.isCompleted) {
          try {
            val keys = consulApi.pollingRead(configRootPath)
            if(keys != null && !cancellation.isCompleted)
              rebuild(keys)
          } catch {
            case e: java.util.concurrent.ExecutionException =>
              e.getCause match {
                case _: java.util.concurrent.TimeoutException =>
                  // long poll timeout, keep going
                case _: java.net.ConnectException =>
                  // Connection refused.  stop
                  done = true
                case _ =>
                  logger.info("Error in property fetching loop", e)
                  Thread.sleep(3000)
              }
            case e: Exception =>
              logger.info("Error in property fetching loop", e)
              Thread.sleep(3000)
            case e: Throwable =>
              logger.info("Exiting property reading loop", e)
              done = true
          }
        }
      }
    }
  }

  private def rebuild(keys: Array[ConsulKey]): Unit = {
    val newSettings = (for {
    //ns <- defaultKeyStores;
      k <- keys
      if !k.Key.endsWith("/")  // directory is listed as ending with "/", skip them
    } yield ("/"+k.Key, k.decodedValue)).
      toMap

    // assign settings map before processing change events
    settings = newSettings

    newSettings.foreach(kv => {
      events.onNext(KeyValuePair(kv._1,kv._2))
    })
    logger.info(s"Refreshed at index: ${consulApi.index}")
  }
}

object Dconfig extends StrictLogging {
  // typesafe config to check for bootstrap settings.
  private lazy val typesafeConfig = ConfigFactory.load()
  // storage of a stub implementation for unit testing
  private var stub: Option[ConfigSettings] = None

  // the singleton instance of ConfigSettings
  private lazy val instance: ConfigSettings = {
    isInitialized = true
    if (stub.isDefined)
      stub.get
    else if (typesafeConfig.hasPath("dconfig.consul.url") && typesafeConfig.getString("dconfig.consul.url") != "")
      new Dconfig
    else {
      logger.error("Failed to find setting 'dconfig.consul.url'. Falling back to Typesafe based ConfigSettings.")
      new TypesafeConfigSettings
    }
  }
  // track if the singleton instance has been created yet.
  private var isInitialized = false

  /**
    * For unit testing, pass an alternate implementation of ConfigSettings prior to first call to Dconfig()
    * @param s alternate ConfigSettings implementation to use.
    */
  def stub(s: ConfigSettings): Unit = {
    // add check for instance already created
    if (isInitialized)
      throw new Exception("Dconfig ConfigSettings instance is already inititalized!")

    stub = Some(s)
  }

  def apply(): ConfigSettings = instance
}

// Do NOT make it inner class, because serialization (at least jackson) will fail to create instance of object

case class ConsulKey(
@JsonProperty("CreateIndex") CreateIndex: Long,
@JsonProperty("ModifyIndex") ModifyIndex: Long,
@JsonProperty("LockIndex") LockIndex: Long,
@JsonProperty("Key") Key: String,
@JsonProperty("Flags") Flags: Long,
@JsonProperty("Value") Value: String
) {
  def this(kv: (String,String)) = this(0L, 0L, 0L, kv._1, 0L,
    if(kv._2 == null) "" else Base64.encodeBase64String(StandardCharsets.UTF_8.encode(kv._2).array()))
  val decodedValue: String = if(Value == null || Value == "") "" else new String(Base64.decodeBase64(Value))
}
