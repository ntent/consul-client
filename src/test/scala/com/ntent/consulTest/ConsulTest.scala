package com.ntent.consulTest

import java.io.File
import java.net.InetAddress
import java.util.concurrent.TimeoutException

import com.ntent.configuration.{ConsulApiImplDefault, Dconfig}
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.scalatest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.sys.process._
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by vchekan on 2/3/2016.
  */
class ConsulTest extends FlatSpec with ShouldMatchers with OneInstancePerTest with BeforeAndAfterAllConfigMap with BeforeAndAfterEach {
  val rootFolder = "test/app1"
  //System.setProperty("dconfig.consul.url", "http://mw-01.lv.ntent.com:8500/")
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO")

  var consulProcess: Process = null

  override def beforeAll(conf: ConfigMap): Unit = {
    val exe = if(System.getProperty("os.name").toLowerCase.contains("windows")) "consul.exe" else "consul"
    consulProcess = Seq("bin/"+exe, "agent", "-advertise", "127.0.0.1", "-config-file", "bin/config.json").run()
    Thread.sleep(8000)

    val api = new com.ntent.configuration.ConsulApiImplDefault()
    devSettings.foreach(kv => api.put(rootFolder + "/dev", kv._1, kv._2))
    stgSettings.foreach(kv => api.put(rootFolder + "/stg", kv._1, kv._2))
  }

  override def afterAll(conf: ConfigMap): Unit = {
    cleanup()
  }

  override def beforeEach() = {
    System.clearProperty("dconfig.consul.keyStores")
    System.setProperty("dconfig.consul.configRoot", rootFolder)
    ConfigFactory.invalidateCaches()
  }

  def cleanup() = {
    if(consulProcess != null) {
      consulProcess.destroy()
      Thread.sleep(500)
      consulProcess = null
    }
    if(new File("bin/consul-data").exists())
      FileUtils.deleteDirectory(new File("bin/consul-data"))
  }

  val devSettings = Seq(
    "key1" -> "value1",
    "key2" -> "value2",
    "folder1/" -> null,
    "folder1/key1" -> "folder value1"
  )

  val stgSettings = Seq(
    "key_new" -> "new value",
    "key2" -> "value2 override"
  )

  val host = java.net.InetAddress.getLocalHost.getHostName
  val localSettings = Seq(s"$rootFolder/${host}/key1" -> "local value")

  "Consul" should "list keys under root" in {
    val dc = new Dconfig()
    val res = dc.get("key1")
    assert(res == "value1")
  }

  it should "select right value if key defined in 2 namespaces" in {
    System.setProperty("dconfig.consul.keyStores", "stg dev {host}")
    try {
      ConfigFactory.invalidateCaches()
      val dc = new Dconfig()
      val res = dc.get("key2")
      assert(res == "value2")
    } finally {
      System.clearProperty("dconfig.consul.keyStores")
    }
  }

  it should "select right value if key defined in 2 namespaces (reverse order from previous test)" in {
    // TODO: how to move property update and invalidation out of the test?
    System.setProperty("dconfig.consul.keyStores", "{host} dev stg")
    try {
      ConfigFactory.invalidateCaches()

      val dc = new Dconfig()
      val res = dc.get("key2")
      assert(res == "value2 override")
    } finally {
      System.clearProperty("dconfig.consul.keyStores")
    }
  }

  it should "execute action when key change (live update)" in {
    val dc = new Dconfig()
    val p = Promise[String]()

    dc.liveUpdate("liveKey").
      subscribe(v => p.trySuccess(v))

    val api = new ConsulApiImplDefault()
    val value = "live value-" + new java.util.Random().nextLong().toString
    api.put(rootFolder, "dev/liveKey", value)

    val res = Await.result(p.future, Duration(30, "seconds"))
    assert(res == value)
  }

  it should "not trigger keys which were not subscribed to" in {
    val dc = new Dconfig()
    val p = Promise[String]()

    dc.liveUpdate("NoSuchSettingsExist").
      subscribe(v => p.trySuccess(v))

    intercept[TimeoutException] {
      val res = Await.result(p.future, Duration(10, "seconds"))
    }
  }

  it should "not trigger live update when same key but different namespace is changed" in {
    val dc = new Dconfig()
    val api = new ConsulApiImplDefault()
    val p = Promise[String]()

    // Listen in "live" namespace
    dc.liveUpdate("deadKey", "live").
      subscribe(v => p.trySuccess(v))

    // perform update in 2 seconds in "dead" namespace
    Future({ Thread.sleep(2000); api.put(rootFolder, "dead/deadKey", "dead update")})

    intercept[TimeoutException] {
      val res = Await.result(p.future, Duration(10, "seconds"))
      assert(false, s"No result expected but got: '${res}'")
    }
  }

  it should "return None on non-existing key" in {
    val dc = new Dconfig()
    intercept[RuntimeException] {
      val res = dc.get("no-such-key")
    }
  }

  it should "not throw if root namespace does not exist" in {
    val ns = "dev stg nosuchnamespace"
    System.setProperty("dconfig.consul.keyStores", ns)
    System.setProperty("dconfig.consul.configRoot", rootFolder + "/nosuchroot")
    ConfigFactory.invalidateCaches()
    val dc = new Dconfig()
    assert(ns == dc.keystores.mkString(" "))
  }

  it should "override value for local host" in {
    val dc = new Dconfig
    val api = new ConsulApiImplDefault
    val host = InetAddress.getLocalHost.getHostName
    val value = "local value"
    api.put(rootFolder, s"$host/key1", value)

    Thread.sleep(5000)
    val got = dc.get("key1")
    assert(got == value)
  }

  /*it should "not leak threads" in {
    val dc = Dconfig()
    Thread.sleep(5000000)
  }*/
}
