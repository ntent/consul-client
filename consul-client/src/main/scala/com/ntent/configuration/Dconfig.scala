package com.ntent.configuration

import com.typesafe.scalalogging.StrictLogging
import rx.lang.scala.{Observable, Subject}
import rx.lang.scala.schedulers.ExecutionContextScheduler
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.annotation.JsonProperty
import com.typesafe.config.ConfigFactory
import org.apache.commons.codec.binary.Base64

import scala.collection.mutable
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
  @volatile private var defaultKeyStores = defKeyStores
  if(defKeyStores.nonEmpty) {
    defaultKeyStores = expandAndReverseNamespaces(defKeyStores.toArray)
  } else {
    defaultKeyStores = expandAndReverseNamespaces(appSettings.getString("dconfig.consul.keyStores").split(" |,|\\|"))
  }
  override val keystores: Seq[String] = defaultKeyStores.reverse

  /** Subscribe to receive notice of Console value changes. */
  private val updateEvents = Subject[KeyValuePair]()
  @volatile private var settings: scala.collection.Map[String,String] = Map.empty[String,String]
  @volatile private var readLoopFatal:Throwable = _
  private val readingLoopTask = Promise[Boolean]

  {

    // initialize a print-out of changes. this subscription doesn't print until there are two entries (a new value changed)
    // except it does flush when the subscription ends (even if the value never changed)
    updateEvents.groupBy(kv=>kv.fullPath).flatMap(kv=>kv._2.slidingBuffer(2,1)).subscribe(kvs => {
      if (kvs.length == 2)
        logger.info(s"Changed config: ${kvs.head.fullPath} : ${kvs.head.value} => ${kvs(1).value}")
    }, e=> logger.error("Error in config change subscription. No longer printing configuration updates.",e))

    initialRead()

    startReadingLoop(readingLoopTask.future)

  }

  def this() {
    this(ConfigFactory.load().getString("dconfig.consul.configRoot").stripMargin('/') + '/')
  }

  private[configuration] def close() = {
    updateEvents.onCompleted()
    readingLoopTask.complete(Success(true))
  }

  private def ensureOpen(): Unit = {
    if (readLoopFatal != null)
      throw readLoopFatal
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
    } yield KeyValuePair(ns + "/" + key, s.get, key)).headOption
  }

  override def getKeyValuePairsAt(namespace: String): Set[KeyValuePair] = {
    ensureOpen()
    val path = "/" + configRootPath + namespace
    settings.filter(p => p._1.startsWith(path) && !p._1.endsWith("/")).map(f => {
      val key = f._1.substring(f._1.lastIndexOf("/") + 1)
      KeyValuePair(f._1, f._2, key)
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
    (for {
      key <- settings.keySet
      if key.startsWith(path) && getContainerName(key, path).isDefined
      container <- getContainerName(key, path)
    } yield container).toSet
  }

  private def getContainerName(key: String, rootPath: String): Option[String] = {
    if(key.indexOf("/", rootPath.length + 1) > 0)
      Some(key.substring(rootPath.length, key.indexOf("/", rootPath.length + 1)))
    else None
  }

  override def getEffectiveSettings : Seq[KeyValuePair] = {
    val res = for {
      t <- settings
      settingName = extractKeyFromPath(t._1, defaultKeyStores.toList)
      value = get(settingName, useDefaultKeystores = true)
      if value.isDefined
    } yield value.get
    res.toSeq.distinct
  }

  override def getAllSettings: Seq[KeyValuePair] = {
    val res = for {
      t <- settings
      settingName = extractKeyFromPath(t._1,defaultKeyStores.toList)
    } yield KeyValuePair(t._1,t._2,settingName)
    res.toSeq
  }

  override def liveUpdateAll(): Observable[KeyValuePair] = {
    liveUpdateFolder("")
  }

  override def liveUpdateFolder(folder: String): Observable[KeyValuePair] = {
    var folderWithRoot = s"/$configRootPath$folder"
    if (!folderWithRoot.endsWith("/")) folderWithRoot += "/"

    ensureOpen()
    val uniqueChanges = updateEvents
    // only changes that are a sub-path of the given folder.
    uniqueChanges.withFilter(kv => kv.fullPath.startsWith(folderWithRoot))
      .map(kv=>kv)
      .observeOn(ExecutionContextScheduler(scala.concurrent.ExecutionContext.global))
  }

  override def liveUpdateEffectiveSettings() : Observable[KeyValuePair] = {
    ensureOpen()

    // distinct will only alert us of changes, but we want to emit all values when first subscribed to.
    // concatenate the existing keys/values with the upcoming changes.
    val ob = Observable.from(settings).map(t=>KeyValuePair(t._1,t._2, extractKeyFromPath(t._1, defaultKeyStores.toList))) ++ updateEvents

    // get just the key of the setting names (removing the path data)
    val res:Observable[KeyValuePair] = for {
      kv <- ob
      value = get(kv.key, useDefaultKeystores = true)
      if value.isDefined
    } yield value.get

    res.groupBy(kv=>kv.fullPath).flatMap(kv=>kv._2.distinctUntilChanged)
      .observeOn(ExecutionContextScheduler(scala.concurrent.ExecutionContext.global))
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
    val uniqueChanges = updateEvents
    uniqueChanges
      // only the config roots we are using
      .withFilter(kv => {
        val res = trackingPaths.contains(kv.fullPath)
        if(res)
          logger.debug(s"Tracking paths contains '${kv.fullPath}' with value '${kv.value}'.")
        res
      })
      // For this key, get the latest value (use root precedence)
      .map(_=> get(key, useDefaultKeystores, namespaces:_*))
      // If there is a value
      .withFilter(_.isDefined)
      // Get the KeyValuePair.
      .map(_.get)
      // Stop if you've heard this one before.
      .distinctUntilChanged
      // Log the value we're using now.
      .doOnNext(kv => logger.info(s"Now using: ${kv.fullPath} : ${kv.value}"))
      //.observeOn(TrampolineScheduler())
      // Broadcast the new value to subscribers.
      .observeOn(ExecutionContextScheduler(scala.concurrent.ExecutionContext.global))
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
        var sleep = false
        while(!done && !cancellation.isCompleted) {

          try {
            if (sleep) {
              sleep = false
              Thread.sleep(3000)
            }

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
                  logger.error("ConnectException in property fetching loop. Retrying", e)
                  sleep = true
                case _ =>
                  logger.info("Other Exception in property fetching loop. Retrying", e)
                  sleep = true
              }
            case e: Exception =>
              logger.warn("Exception in property fetching loop. Retrying", e)
              sleep = true
            case t: Throwable =>
              readLoopFatal = t
              done = true
              logger.error("Caught throwable in Property ReadingLoop Exiting.", t)
          }
        }
      }
    }
  }

  private def rebuild(keys: Array[ConsulKey]): Unit = {
    // Mutable map lookup is 4x faster than immutable map lookup.  (Uses hash tables, versus trees.)
    // http://www.lihaoyi.com/post/BenchmarkingScalaCollections.html#lookup-performance

    // Map from each fullPath, to each value.
    val newSettings = new mutable.HashMap[String, String]()
    val oldSettings = settings
    // Any new key/value pairs we need to broadcast to subscribers.
    val updates = new mutable.ArrayBuffer[KeyValuePair]()

    // For each Consul key-value pair,
    for {
      consulKey <- keys
      if !consulKey.Key.endsWith("/")  // directory is listed as ending with "/", skip them
    } {
      val fullPath = "/" + consulKey.Key  // Start path-key with a slash.
      val newValue = consulKey.decodedValue
      // If there is a previous value for this fullPath, and it is the "same" as this value,
      val prevValue = oldSettings.getOrElse(fullPath, null)
      if (newValue == prevValue) {
        // Store old value, in new map.  (Useful, if clients are comparing values from map.)
        newSettings.put(fullPath, prevValue)
      } else {
        // Store the new value.
        newSettings.put(fullPath, newValue)
        // Record this update.
        val key = extractKeyFromPath(fullPath, defaultKeyStores.toList)
        updates += KeyValuePair(fullPath, newValue, key)
      }
    }

    // Switch to new map, before broadcasting changes to subscribers.
    settings = newSettings

    // Broadcast new values.
    updates.foreach(kv => updateEvents.onNext(kv))

    // Also broadcast paths that no longer have values.
    oldSettings.keys.filterNot(fp => newSettings.contains(fp)).foreach(fullPath => {
      val key = extractKeyFromPath(fullPath, defaultKeyStores.toList)
      updateEvents.onNext(KeyValuePair(fullPath, null, key))
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
