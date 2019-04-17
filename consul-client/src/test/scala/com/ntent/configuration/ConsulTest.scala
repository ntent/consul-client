package com.ntent.configuration

import java.io.File
import java.net.InetAddress
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.ConfigFactory
import com.ntent.configuration.ConsulTest._

import org.apache.commons.io.FileUtils
import org.scalatest._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.sys.process._

/**
  * Created by vchekan on 2/3/2016.
  */
class ConsulTest extends FlatSpec with Matchers with OneInstancePerTest with BeforeAndAfterAllConfigMap with BeforeAndAfterEach {
  // set up our keystores
  val rootFolder = "test/app1"
  System.setProperty("dconfig.consul.keyStores","global dev {host}")
  //System.setProperty("dconfig.consul.url", "http://mw-01.lv.ntent.com:8500/")
  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO")

  var consulProcess: Process = _

  override def beforeAll(conf: ConfigMap): Unit = {

    val exe = if(System.getProperty("os.name").toLowerCase.contains("windows")) "consul.exe" else "consul"
    consulProcess = Seq("bin/"+exe, "agent", "-advertise", "127.0.0.1", "-config-file", "bin/config.json").run()
    Thread.sleep(8000)

    val api = new com.ntent.configuration.ConsulApiImplDefault()
    api.deleteTree(rootFolder)
    //noinspection ScalaUnusedSymbol
    val ttt = api.read(rootFolder)
    devSettings.foreach(kv => api.put(rootFolder + "/dev", kv._1, kv._2))
    stgSettings.foreach(kv => api.put(rootFolder + "/stg", kv._1, kv._2))
    globalSettings.foreach(kv=>api.put(rootFolder + "/global", kv._1, kv._2))
  }

  override def afterAll(conf: ConfigMap): Unit = {
    cleanup()
  }

  override def beforeEach(): Unit = {
    System.setProperty("dconfig.consul.keyStores","global dev {host}")
    System.setProperty("dconfig.consul.configRoot", rootFolder)
    ConfigFactory.invalidateCaches()
  }

  override def afterEach(): Unit = {
    System.clearProperty("dconfig.consul.keyStores")
  }

  def cleanup(): Unit = {
    if(consulProcess != null) {
      consulProcess.destroy()
      Thread.sleep(500)
      consulProcess = null
    }
    if(new File("bin/consul-data").exists())
      FileUtils.deleteDirectory(new File("bin/consul-data"))
  }

  private val globalSettings = Seq(
    "liveKey" -> "global-value",
    "key2" -> "global-value",
    "localKey" -> "global-value",
    "globalKey" -> "global-value"
  )

  private val devSettings = Seq(
    "key1" -> "value1",
    "key2" -> "value2",
    "intKey" -> "1",
    "longKey" -> "4611686018427387903",
    "trueBoolKey" -> "true",
    "falseBoolKey" -> "false",
    "doubleKey" -> "1.234",
    "folder1/" -> null,
    "folder1/key1" -> "folder value1",
    "liveKey" -> "dev-value",
    "localKey" -> "dev-value"
  )

  private val stgSettings = Seq(
    "key_new" -> "new value",
    "key2" -> "value2 override",
    "liveKey" -> "stg-value",
    "localKey" -> "stg-value"
  )

  "Consul" should "list keys under root" in {
    val dc = new Dconfig()
    val res = dc.get("key1")
    assert(res == "value1")
    dc.close()
  }

  it should "get correct effective settings" in {
    System.setProperty("dconfig.consul.keyStores", "global dev stg {host}")
    ConfigFactory.invalidateCaches()

    val dc = new Dconfig()
    val effective = dc.getEffectiveSettings
    assert(effective.length == 12)
    assert(effective.filter(kv => kv.key == "globalKey").head.value == "global-value")
    assert(effective.filter(kv => kv.key == "liveKey").head.value == "stg-value")
    assert(effective.filter(kv => kv.key == "localKey").head.value == "stg-value")
    assert(effective.filter(kv => kv.key == "intKey").head.value == "1")
  }

  it should "list all keys under stg container" in {
    val dc = new Dconfig()
    val keyValPairs = dc.getKeyValuePairsAt("stg")
    keyValPairs.foreach(p => assert(stgSettings.contains((p.key, p.value))))
  }

  it should "convert known datatypes correctly" in {
    val dc = new Dconfig()
    assert(dc.getAs[Int]("intKey") == 1)
    assert(dc.getAs[Long]("longKey") == 4611686018427387903L)
    assert(dc.getAs[Boolean]("trueBoolKey"))
    assert(!dc.getAs[Boolean]("falseBoolKey"))
    assert(dc.getAs[Double]("doubleKey") == 1.234)
    dc.close()
  }

  it should "select right value if key defined in 2 namespaces" in {
    System.setProperty("dconfig.consul.keyStores", "global stg dev {host}")
    ConfigFactory.invalidateCaches()

    val dc = new Dconfig()
    val res = dc.get("key2")
    assert(res == "value2")
    dc.close()
  }

  it should "select right value if key defined in 2 namespaces (reverse order from previous test)" in {
    System.setProperty("dconfig.consul.keyStores", "{host} dev stg")
    ConfigFactory.invalidateCaches()

    val dc = new Dconfig()
    val res = dc.get("key2")
    assert(res == "value2 override")
    dc.close()
  }

  it should "live update only changed value when changed" in {
    val dc = new Dconfig()
    val p = Promise[KeyValuePair]()

    val val1 = KeyValuePair("sub/foo","bar","foo")
    val val2 = KeyValuePair("sub/foo","bar","foo")
    assert(val1 == val2)
    assert(val1.hashCode() == val2.hashCode())

    dc.liveUpdate("liveKey").
      subscribe(v => {
        if (p.isCompleted)
          p.tryFailure(new RuntimeException("liveUpdate called more than once for a single key update"))
        else
          p.trySuccess(v)
      })

    val api = new ConsulApiImplDefault()
    val value = "live value-" + nextLongId()
    api.put(rootFolder, "dev/liveKey", value)

    val res = Await.result(p.future, Duration(30, "seconds"))
    assert(res.fullPath == "dev/liveKey")
    assert(res.value == value)
    dc.close()
  }

  it should "live update custom namespaces" in {
    val dc = new Dconfig()
    val p = Promise[KeyValuePair]()

    dc.liveUpdate("liveKey","custom1","custom2").
      subscribe(v => {
        if (p.isCompleted)
          p.tryFailure(new RuntimeException("liveUpdate called more than once for a single key update"))
        else
          p.trySuccess(v)
      })

    val api = new ConsulApiImplDefault()
    val value = "live value-" + nextLongId()
    api.put(rootFolder, "custom2/liveKey", value)

    val res = Await.result(p.future, Duration(5, "seconds"))
    assert(res.fullPath == "custom2/liveKey")
    assert(res.value == value)
    dc.close()
  }

  it should "update to parent value, after child key delete" in {
    val key = s"test${nextLongId()}liveKey"
    val globalValue = "globalValue"
    val devValue1 = "devValue1"
    val devValue2 = "devValue2"
    val api = new ConsulApiImplDefault()
    println("Initialize devValue1, which will hide globalValue.")
    api.put(rootFolder, s"global/$key", globalValue)
    api.put(rootFolder, s"dev/$key", devValue1)

    Thread.sleep(2000)
    val dc = new Dconfig()
    println(s"get($key) returned: ${dc.get(key)}")
    @volatile var nextPromise = 0
    val promises = Array.fill(3){ Promise[KeyValuePair] }
    dc.liveUpdate(key).subscribe(v => {
      println(s"subscription($nextPromise) received: $v")
      promises(nextPromise).trySuccess(v)
      nextPromise += 1
    })

    println("Remove devValue1, revealing globalValue.")
    api.deleteKey(rootFolder, s"dev/$key")
    println("Waiting on subscription(0)")
    var kv = Await.result(promises(0).future, Duration(5, "seconds"))
    println(s"get($key) returned: ${dc.get(key)}")
    assert(kv.value == globalValue)

    println("Set devValue2, which will hide globalValue.")
    api.put(rootFolder, s"dev/$key", devValue2)
    try {
      println("Waiting on subscription(1)")
      kv = Await.result(promises(1).future, Duration(5, "seconds"))
    } finally {
      println(s"get($key) returned: ${dc.get(key)}")
    }
    assert(kv.value == devValue2)

    println("Remove devValue2, revealing globalValue.")
    api.deleteKey(rootFolder, s"dev/$key")
    try {
      println("Waiting on subscription(2)")
      kv = Await.result(promises(2).future, Duration(5, "seconds"))
    } finally {
      println(s"get($key) returned: ${dc.get(key)}")
    }
    assert(kv.value == globalValue)

    dc.close()
  }

  it should "not live update when key change in parent namespace" in {
    val dc = new Dconfig()
    val p = Promise[KeyValuePair]
    val pSecond = Promise[KeyValuePair]

    dc.liveUpdate("liveKey").
      subscribe(v => {
        if (p.isCompleted)
          pSecond.trySuccess(v)
        else
          p.trySuccess(v)
      })

    val api = new ConsulApiImplDefault()
    val devValue = "live value-" + nextLongId()
    val globalValue = "live value-" + nextLongId()
    api.put(rootFolder, "dev/liveKey", devValue)

    val res = Await.result(p.future, Duration(30, "seconds"))
    assert(res.value == devValue)

    // this should complete second if subscription is called a second time.
    api.put(rootFolder, "global/liveKey", globalValue)

    intercept[TimeoutException] {
      //noinspection ScalaUnusedSymbol
      val res = Await.result(pSecond.future, Duration(3, "seconds"))
    }

    dc.close()
  }

  it should "live update when any key change if watching root" in {
    val dc = new Dconfig()
    val p = Promise[String]()

    dc.liveUpdateAll().
      subscribe(v => p.trySuccess(v.value))

    val api = new ConsulApiImplDefault()
    val value = "live value-" + nextLongId()
    api.put(rootFolder, "dev/liveKey", value)

    //noinspection ScalaUnusedSymbol
    val res = Await.result(p.future, Duration(30, "seconds"))

    dc.close()
  }

  it should "liveUpdateEffectiveSettings on all keys at start" in {
    val dc = new Dconfig()
    @volatile var count = 0
    dc.liveUpdateEffectiveSettings().
      doOnEach(kv=>Console.println(s"${kv.fullPath} = ${kv.value}")).
      subscribe(_ => count = count+1)
    Thread.sleep(500)
    assert(count > 1,"Did not receive more than one updated key!")

    dc.close()
  }

  it should "liveUpdateEffectiveSettings once with new value when a key is changed" in {
    val dc = new Dconfig()
    @volatile var liveValue = ""
    @volatile var count = 0
    dc.liveUpdateEffectiveSettings().
      filter(kv=>kv.key=="liveKey").
      doOnEach(kv=>Console.println(s"${kv.fullPath} = ${kv.value}")).
      subscribe(kv => { count = count+1; liveValue = kv.value })

    Thread.sleep(500)
    assert(liveValue != "","Did not receive initial value for liveKey!")
    assert(count == 1,s"Did not receive single update for liveKey! (saw $count updates)")

    val api = new ConsulApiImplDefault()
    var value = "live value-" + nextLongId()
    api.put(rootFolder, "dev/liveKey", value)

    Thread.sleep(2000)
    assert(liveValue == value, s"Expected live update to $value but got $liveValue")
    assert(count == 2,s"Did not receive second update for liveKey! (saw $count updates)")

    value = "live value-" + nextLongId()
    api.put(rootFolder, "dev/liveKey", value)

    Thread.sleep(2000)
    assert(liveValue == value, s"Expected live update to $value but got $liveValue")
    assert(count == 3,s"Did not receive third update for liveKey! (saw $count updates)")

    dc.close()
  }

  it should "not live update on keys which were not subscribed to" in {
    val dc = new Dconfig()
    val p = Promise[String]()

    dc.liveUpdate("NoSuchSettingsExist").
      subscribe(v => p.trySuccess(v.value))

    val api = new ConsulApiImplDefault()
    val value = "live value-" + nextLongId()
    api.put(rootFolder, "dev/liveKey", value)

    intercept[TimeoutException] {
      //noinspection ScalaUnusedSymbol
      val res = Await.result(p.future, Duration(3, "seconds"))
    }
    dc.close()
  }

  it should "not live update on same key but different namespace" in {
    val dc = new Dconfig()
    val api = new ConsulApiImplDefault()
    val p = Promise[String]()

    // Listen in "live" namespace
    dc.liveUpdate("deadKey", "live").
      subscribe(v => p.trySuccess(v.value))

    // perform update in 2 seconds in "dead" namespace
    val value = "dead update-" + nextLongId()
    Future({ Thread.sleep(2000); api.put(rootFolder, "dead/deadKey", value)})

    intercept[TimeoutException] {
      val res = Await.result(p.future, Duration(3, "seconds"))
      fail(s"No result expected but got: '$res'")
    }
    dc.close()
  }

  it should "return None on non-existing key" in {
    val dc = new Dconfig()
    intercept[RuntimeException] {
      //noinspection ScalaUnusedSymbol
      val res = dc.get("no-such-key")
    }
    dc.close()
  }

  it should "not throw if root namespace does not exist" in {
    val ns = "dev stg nosuchnamespace"
    System.setProperty("dconfig.consul.keyStores", ns)
    System.setProperty("dconfig.consul.configRoot", rootFolder + "/nosuchroot")
    ConfigFactory.invalidateCaches()

    val dc = new Dconfig()
    assert(ns == dc.keystores.mkString(" "))
    dc.close()
  }

  it should "override value for local host" in {
    val dc = new Dconfig()
    val api = new ConsulApiImplDefault
    val host = InetAddress.getLocalHost.getHostName
    val value = "local value-" + nextLongId()
    val key = "localKey"
    api.put(rootFolder, s"$host/$key", value)

    Thread.sleep(2000)
    val got = dc.get(key)
    assert(got == value)
    dc.close()
  }

  it should "return keys for provided root" in {
    val customRoot = "MarketConfig/markets"
    val api = new ConsulApiImplDefault
    val markets = Set("Guam", "Kazakhstan", "Arizona")
    for (market <- markets) {
      api.put(rootFolder, s"$customRoot/$market/$market.whitelist", s"$market whitelist")
    }
    api.put(rootFolder, s"$customRoot/foo", "random value")
    api.put(rootFolder, s"$customRoot/testFolder/", null)
    Thread.sleep(2000)
    val dc = new Dconfig(rootFolder)

    val marketSet = dc.getChildContainersAt(customRoot)
    assert(marketSet == markets)
    dc.close()
  }

  it should "not return containers more than one level down" in {
    val customRoot = "MarketConfig/markets"
    val api = new ConsulApiImplDefault
    api.put(rootFolder, s"$customRoot/Fake/tooFar/tooFarKey", "foo")
    Thread.sleep(2000)
    val dc = new Dconfig(rootFolder)
    val marketSet = dc.getChildContainersAt(customRoot)
    assert (marketSet.contains("Fake") && !marketSet.contains("tooFar"))
    dc.close()
  }

  ignore should "use acl token if specified in settings" in {
    System.setProperty("dconfig.consul.url", "http://dev-consul-01.cb.ntent.com:8500/")
    System.setProperty("dconfig.consul.access.token", "user-e2bbfe84-7dd0-47e7-ac34-849e96272b64")
    ConfigFactory.invalidateCaches()

    val api = new com.ntent.configuration.ConsulApiImplDefault()

    val ttt = api.read("test/java-dconfig-acl-test/read-value")
    assert(ttt.length == 1)
    assert(ttt.head.decodedValue == "foo")

    val rnd = nextLongId()
    api.put("test/java-dconfig-acl-test","write-value",rnd)

    val readWritten = api.read("test/java-dconfig-acl-test/write-value")
    assert(readWritten.length == 1)
    assert(readWritten.head.decodedValue == rnd)

  }

  /*it should "not leak threads" in {
    val dc = Dconfig()
    Thread.sleep(5000000)
  }*/
}

object ConsulTest {
  val lastLongRef: AtomicLong = new AtomicLong(0)
  /** Unique during this test run. */
  def nextLongId(): String = {
    lastLongRef.incrementAndGet().toString
  }
}
