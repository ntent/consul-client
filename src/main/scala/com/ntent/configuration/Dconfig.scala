package com.ntent.configuration

import com.typesafe.scalalogging.slf4j.StrictLogging
import rx.lang.scala.{Observable, Subject}
import rx.lang.scala.schedulers.ExecutionContextScheduler
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.annotation.JsonProperty
import com.sun.javaws.exceptions.InvalidArgumentException
import com.typesafe.config.ConfigFactory
import org.apache.commons.codec.binary.Base64

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.blocking
import scala.reflect.runtime.universe._

/**
  * Created by vchekan on 2/3/2016.
  */
class Dconfig(rootPath: String, defKeyStores: String*) extends StrictLogging {
  private val appSettings = ConfigFactory.load()
  private val _hostFQDN = java.net.InetAddress.getLocalHost.getHostName
  //Make sure configRoot path doesn't have a leading '/' and ends with a single '/'
  lazy val configRootPath = rootPath.stripMargin('/').stripSuffix("/") + '/'
  val consulApi: ConsulApiImplDefault = new ConsulApiImplDefault()
  private var defaultKeyStores = defKeyStores
  if(defKeyStores.length > 0) {
    defaultKeyStores = expandAndReverseNamespaces(defKeyStores.toArray)
  } else {
    defaultKeyStores = expandAndReverseNamespaces(appSettings.getString("dconfig.consul.keyStores").split(" |,|\\|"))
  }
  val keystores = defaultKeyStores.reverse

  private var settings = initialRead()

  private val readingLoopTask = startReadingLoop()

  private lazy val events = Subject[(String,String)]()

  def this() {
    this(ConfigFactory.load().getString("dconfig.consul.configRoot").stripMargin('/') + '/')
  }

  /** Return (value, namespace) */
  def getWithNamespace(key: String): Option[(String,String)] = get(key, true)

  def get(key: String): String = get(key, true).getOrElse(throw new RuntimeException(s"Key not found '$key'"))._1

  def getAs[T : TypeTag](key:String): T = {
    convert[T](get(key))
  }

  def convert[T : TypeTag](value:String):T = {
    typeOf[T] match {
      case t if t =:= typeOf[String] => value.asInstanceOf[T]
      case t if t =:= typeOf[Int] => value.toInt.asInstanceOf[T]
      case t if t =:= typeOf[Long] => value.toLong.asInstanceOf[T]
      case t if t =:= typeOf[Double] => value.toDouble.asInstanceOf[T]
      case t if t =:= typeOf[Boolean] => value.toBoolean.asInstanceOf[T]
      case t if t =:= typeOf[Byte] => value.toByte.asInstanceOf[T]
      case _ => throw new IllegalArgumentException("Cannot convert to type " + typeOf[T].toString)
    }
  }

  def get(key: String, useDefaultKeystores: Boolean, namespaces: String*): Option[(String,String)] = {
    val allNamespaces = if(useDefaultKeystores) namespaces.reverse ++ defaultKeyStores else namespaces.reverse
    (for {
      ns <- allNamespaces
      path = "/" + configRootPath + ns + "/" + key
      s = settings.get(path)
      if(s.isDefined)
    } yield (s.get, ns)).headOption
  }

  def getChildContainers(): Set[String] = {
    val path = "/" + configRootPath
    getChildContainers(path)
  }

  def getChildContainersAt(namespace: String): Set[String] = {
    val path = "/" + configRootPath + namespace + "/"
    getChildContainers(path)
  }

  private def getChildContainers(path: String): Set[String] = {
     for {
      key <- settings.keySet
      if (key.startsWith(path) && getContainerName(key, path).isDefined)
        container <- getContainerName(key, path)
    } yield container
  }

  private def getContainerName(key: String, rootPath: String): Option[String] = {
    if(key.indexOf("/", rootPath.length + 1) > 0)
      Some(key.substring(rootPath.length, key.indexOf("/", rootPath.length + 1)))
    else None
  }

  def liveUpdate(key: String, namespaces: String*): Observable[String] = {
    var trackingPaths = Set("/" + configRootPath + key)
    if(defaultKeyStores.nonEmpty)
     trackingPaths = (for(ns <- (namespaces.reverse ++ defaultKeyStores))
      yield "/" + configRootPath + ns + "/" + key
      ).toSet

    logger.info(s"listening to paths: ${trackingPaths}")

    events.withFilter(kv => {
      val res = trackingPaths.contains(kv._1)
      if(res)
        logger.info(s"Live path '${trackingPaths(kv._1)}' contains '${kv}'. Sending to 'distinct()' filter")
      res
    }).
      map(_._2).
      distinctUntilChanged.
      doOnNext(v => logger.info(s"Distinct update confing: ${v}")).
      //observeOn(TrampolineScheduler())
      observeOn(ExecutionContextScheduler(scala.concurrent.ExecutionContext.global))
  }

  private def initialRead() = {
    val keys = consulApi.read(configRootPath)
    rebuild(keys)
  }

  private def startReadingLoop() = {
    scala.concurrent.Future {
      blocking {
        // TODO: handle shutdown
        var done = false
        while(!done) {
          try {
            val keys = consulApi.pollingRead(configRootPath)
            if(keys != null)
              settings = rebuild(keys)
          } catch {
            case e: java.util.concurrent.ExecutionException => {
              val cause = e.getCause
              if (cause.isInstanceOf[java.util.concurrent.TimeoutException]) {
                /* long poll timeout, keep going */
              }
              else if(cause.isInstanceOf[java.net.ConnectException]) {
                //Connection refused.  stop
                done = true
              }
              else {
                logger.info("Error in property fetching loop", e)
                Thread.sleep(3000)
              }
            }
            case e: Exception => {
              logger.info("Error in property fetching loop", e)
              Thread.sleep(3000)
            }
            case e: Throwable => {
              logger.info("Exiting property reading loop", e)
              done = true
            }
          }
        }
      }
    }
  }

  private def rebuild(keys: Array[ConsulKey]) = {
    val newSettings = (for {
    //ns <- defaultKeyStores;
      k <- keys
      if(!k.Key.endsWith("/"))  // directory is listed as ending with "/", skip them
    } yield ("/"+k.Key, k.decodedValue)).
      toMap

    newSettings.foreach(kv => {
      events.onNext(kv)
      logger.info(s"Rebuild: $kv")
    })
    logger.debug(s"Refreshed at index: ${consulApi.index}")

    newSettings
  }

  private def expandAndReverseNamespaces(nameSpaces: Array[String]): Array[String] = {
    nameSpaces.map(_.trim).reverse.map(_.replaceAllLiterally("{host}", _hostFQDN))
  }
}

object Dconfig {
  private lazy val instance = new Dconfig()
  def apply() = instance
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
  val decodedValue = if(Value == null || Value == "") "" else new String(Base64.decodeBase64(Value))
}
