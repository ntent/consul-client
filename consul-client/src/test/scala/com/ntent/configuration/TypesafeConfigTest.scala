package com.ntent.configuration

import com.typesafe.config.ConfigFactory
import org.scalatest._

/**
  * Created by tstumpges on 6/5/2017.
  */
class TypesafeConfigTest  extends FlatSpec with Matchers with OneInstancePerTest with BeforeAndAfterAllConfigMap with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    System.setProperty("dconfig.consul.url", "")
    System.setProperty("dconfig.consul.keyStores", "")
    ConfigFactory.invalidateCaches()
  }

  override protected def afterAll(configMap: ConfigMap): Unit = {
    System.clearProperty("dconfig.consul.url")
    System.clearProperty("dconfig.consul.keyStores")
    ConfigFactory.invalidateCaches()
  }

  it should "fall back to TypesafeConfigSettings when no consul url is set" in {
    val dconfig = Dconfig()
    assert(dconfig.isInstanceOf[TypesafeConfigSettings],"expected Dconfig() to return a TypesafeConfigSettings instance!")
  }

  it should "return basic config settings from TypesafeConfigSettings" in {
    System.setProperty("foo.bar.string","baz")
    System.setProperty("foo.bar.int","4")
    System.setProperty("foo.bar.double","1.234")
    System.setProperty("foo.bar.boolean","true")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings
    assertResult("baz","string value")(dconfig.get("foo.bar.string"))
    assertResult(4,"int value")(dconfig.getAs[Int]("foo.bar.int"))
    assertResult(1.234,"double value")(dconfig.getAs[Double]("foo.bar.double"))
    assertResult(true,"bool value")(dconfig.getAs[Boolean]("foo.bar.boolean"))
  }

  it should "use config.rootpath to locate settings" in {
    val rootPath = "some.root.path"
    System.setProperty("config.rootpath", rootPath)
    System.setProperty(s"$rootPath.foo.baz.string", "baz")
    System.setProperty(s"$rootPath.foo.baz.int", "51")
    //"Unreachable" value since it does not have the root path
    System.setProperty("foo.baz.int", "33")
    System.setProperty(s"$rootPath.foo.bar.double","1.234")
    System.setProperty(s"$rootPath.foo.bar.boolean","true")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings
    assertResult("baz","string value")(dconfig.get("foo.baz.string"))
    assertResult(51,"int value")(dconfig.getAs[Int]("foo.baz.int"))
    assertResult(1.234,"double value")(dconfig.getAs[Double]("foo.bar.double"))
    assertResult(true,"bool value")(dconfig.getAs[Boolean]("foo.bar.boolean"))
    System.clearProperty("config.rootpath")
  }

  it should "understand time values" in {
    // Different time values to express "one day".
    val minutesPerDay = 24L * 60
    val secondsPerDay = minutesPerDay * 60
    val msPerDay = secondsPerDay * 1000

    System.setProperty("foo.day.no_units", s"$msPerDay")

    System.setProperty("foo.day.ms", s"${msPerDay}ms")
    System.setProperty("foo.day.ms_plus_1", s"${msPerDay + 1}ms")
    System.setProperty("foo.day.sec", s"${secondsPerDay}s")
    System.setProperty("foo.day.min", s"${minutesPerDay}m")
    System.setProperty("foo.day.hours", "24h")
    System.setProperty("foo.day.days", "1d")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings

    // Rolled out, "no units" no longer acceptable for time values.
    assertThrows[Exception](dconfig.getMs("foo.day.no_units"))

    assert(dconfig.getMs("foo.day.ms")          == msPerDay)
    assert(dconfig.getMs("foo.day.ms_plus_1")   == msPerDay + 1)
    assert(dconfig.getMs("foo.day.sec")         == msPerDay)
    assert(dconfig.getMs("foo.day.min")         == msPerDay)
    assert(dconfig.getMs("foo.day.hours")       == msPerDay)
    assert(dconfig.getMs("foo.day.days")        == msPerDay)

    assert(dconfig.getSec("foo.day.ms")           == secondsPerDay)
    assert(dconfig.getSec("foo.day.ms_plus_1")    == secondsPerDay + 1)
    assert(dconfig.getSec("foo.day.sec")          == secondsPerDay)
    assert(dconfig.getSec("foo.day.min")          == secondsPerDay)
    assert(dconfig.getSec("foo.day.hours")        == secondsPerDay)
    assert(dconfig.getSec("foo.day.days")         == secondsPerDay)

    assert(dconfig.getMinutes("foo.day.ms")               == minutesPerDay)
    assert(dconfig.getMinutes("foo.day.ms_plus_1")        == minutesPerDay + 1)
    assert(dconfig.getMinutes("foo.day.sec")              == minutesPerDay)
    assert(dconfig.getMinutes("foo.day.min")              == minutesPerDay)
    assert(dconfig.getMinutes("foo.day.hours")            == minutesPerDay)
    assert(dconfig.getMinutes("foo.day.days")             == minutesPerDay)
  }

  it should "understand size values" in {
    // Different size values to express "10 gigabytes".
    val mb = 10L * 1024
    val kb = mb * 1024
    val bytes = kb * 1024

    // Until rollout, "no units" acceptable for size values.
    System.setProperty("foo.gig.b_no_units", s"$bytes")
    System.setProperty("foo.gig.kb_no_units", s"$kb")
    System.setProperty("foo.gig.mb_no_units", s"$mb")
    System.setProperty("foo.gig.gb_no_units", "10")

    System.setProperty("foo.gig.b", s"${bytes}b")
    System.setProperty("foo.gig.b_plus_1", s"${bytes + 1}B")
    System.setProperty("foo.gig.k", s"${kb}k")
    System.setProperty("foo.gig.kb", s"${kb}KB")
    System.setProperty("foo.gig.m", s"${mb}m")
    System.setProperty("foo.gig.mb", s"${mb}MB")
    System.setProperty("foo.gig.g", "10g")
    System.setProperty("foo.gig.gb", "10GB")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings

    assert(dconfig.getBytes("foo.gig.b_no_units") == bytes)
    assert(dconfig.getBytes("foo.gig.b")          == bytes)
    assert(dconfig.getBytes("foo.gig.b_plus_1")   == bytes + 1)
    assert(dconfig.getBytes("foo.gig.k")          == bytes)
    assert(dconfig.getBytes("foo.gig.kb")         == bytes)
    assert(dconfig.getBytes("foo.gig.m")          == bytes)
    assert(dconfig.getBytes("foo.gig.mb")         == bytes)
    assert(dconfig.getBytes("foo.gig.g")          == bytes)
    assert(dconfig.getBytes("foo.gig.gb")         == bytes)

    assert(dconfig.getKb("foo.gig.kb_no_units") == kb)
    assert(dconfig.getKb("foo.gig.b")           == kb)
    assert(dconfig.getKb("foo.gig.b_plus_1")    == kb + 1)
    assert(dconfig.getKb("foo.gig.k")           == kb)
    assert(dconfig.getKb("foo.gig.kb")          == kb)
    assert(dconfig.getKb("foo.gig.m")           == kb)
    assert(dconfig.getKb("foo.gig.mb")          == kb)
    assert(dconfig.getKb("foo.gig.g")           == kb)
    assert(dconfig.getKb("foo.gig.gb")          == kb)

    assert(dconfig.getMb("foo.gig.mb_no_units") == mb)
    assert(dconfig.getMb("foo.gig.b")           == mb)
    assert(dconfig.getMb("foo.gig.b_plus_1")    == mb + 1)
    assert(dconfig.getMb("foo.gig.k")           == mb)
    assert(dconfig.getMb("foo.gig.kb")          == mb)
    assert(dconfig.getMb("foo.gig.m")           == mb)
    assert(dconfig.getMb("foo.gig.mb")          == mb)
    assert(dconfig.getMb("foo.gig.g")           == mb)
    assert(dconfig.getMb("foo.gig.gb")          == mb)

    assert(dconfig.getGb("foo.gig.gb_no_units") == 10)
    assert(dconfig.getGb("foo.gig.b")           == 10)
    assert(dconfig.getGb("foo.gig.b_plus_1")    == 10 + 1)
    assert(dconfig.getGb("foo.gig.k")           == 10)
    assert(dconfig.getGb("foo.gig.m")           == 10)
    assert(dconfig.getGb("foo.gig.mb")          == 10)
    assert(dconfig.getGb("foo.gig.g")           == 10)
    assert(dconfig.getGb("foo.gig.gb")          == 10)
  }

  it should "use dotted paths to implement inheritance" in {
    System.setProperty("default.foo.bar.string","default_value")
    System.setProperty("dev.foo.bar.string","dev_value")
    System.setProperty("prd.foo.bar.string","prd_value")
    System.setProperty("dconfig.consul.keyStores","default prd stg dev")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings
    assertResult("dev_value","expected dev value!")(dconfig.get("foo.bar.string"))
  }

  it should "get correct effective settings" in {
    System.setProperty("default.foo.bar.string","default_value")
    System.setProperty("dev.foo.bar.string","dev_value")
    System.setProperty("prd.foo.bar.string","prd_value")
    System.setProperty("dconfig.consul.keyStores","default prd stg dev")
    ConfigFactory.invalidateCaches()
    val dc = new TypesafeConfigSettings

    val effective = dc.getEffectiveSettings
    assert(effective.length == 1)
    assert(effective.filter(kv => kv.key == "foo.bar.string").head.value == "dev_value")
  }

  it should "allow sending a dynamic namespace for inheritance" in {
    System.setProperty("default.foo.bar.string","default_value")
    System.setProperty("dev.foo.bar.string","dev_value")
    System.setProperty("dyn.foo.bar.string","dyn_value")
    System.setProperty("dconfig.consul.keyStores","default dev")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings
    val res = dconfig.get("foo.bar.string",true,"dyn")
    assert(res.isDefined,"expected dyn to have a value!")
    assertResult("dyn_value","expected dyn value!")(dconfig.get("foo.bar.string",true,"dyn").get.value)
//    val res2 = dconfig.get("bar.string",true,"foo dyn")
//    assert(res2.isDefined,"expected dyn to have a value!")
//    assertResult("dyn_value","expected dyn value!")(dconfig.get("bar.string",true,"foo dyn").get.value)
    val res3 = dconfig.get("bar.string",true,"dyn.foo")
    assert(res3.isDefined,"expected dyn to have a value!")
    assertResult("dyn_value","expected dyn value!")(dconfig.get("bar.string",true,"dyn.foo").get.value)
  }

  it should "properly handle slash as separator" in {
    System.setProperty("default.foo.bar.string","default_value")
    System.setProperty("dev.foo.bar.string","dev_value")
    System.setProperty("dyn.foo.bar.string","dyn_value")
    System.setProperty("dconfig.consul.keyStores","default dev")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings
    val res = dconfig.get("foo/bar/string",true,"dyn")
    assert(res.isDefined,"expected dyn to have a value!")
    assertResult("dyn_value","expected dyn value!")(dconfig.get("foo/bar/string",true,"dyn").get.value)
    val res2 = dconfig.get("bar/string",true,"dyn/foo/")
    assert(res2.isDefined,"expected dyn to have a value!")
    assertResult("dyn_value","expected dyn value!")(dconfig.get("bar/string",true,"dyn/foo/").get.value)
  }

  it should "return containers from root" in {
    System.setProperty("default.foo.bar.string","default_value")
    System.setProperty("default.foo.bar.other","default_other_value")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings
    val rootContainers = dconfig.getChildContainers
    assert(rootContainers.contains("default"))
  }

  it should "return containers from a sub-path" in {
    System.setProperty("default.foo.bar.string","default_value")
    System.setProperty("default.foo.bar.other","default_other_value")
    System.setProperty("default.baz.bar.other","default_other_baz_value")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings
    val defaultContainers = dconfig.getChildContainersAt("default")
    assert(defaultContainers.contains("foo"))
    assert(defaultContainers.contains("baz"))
  }

  it should "not treat values as containers" in {
    System.setProperty("default.foo.bar.string","default_value")
    System.setProperty("default.foo.bar.other","default_other_value")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings
    val defaultContainers = dconfig.getChildContainersAt("default.foo.bar")
    assert(defaultContainers.isEmpty)
  }

  it should "not throw exception on liveUpdate calls" in {
    noException should be thrownBy (new TypesafeConfigSettings)
  }

  it should "return uwrapped (unquoted) values from getKeyValuePairsAt" in {
    System.setProperty("default.foo.bat.first","first")
    System.setProperty("default.foo.bat.second","second")
    System.setProperty("default.foo.bat.third","third")
    System.setProperty("default.foo.bat.fourth","fourth")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings
    val keyValPairs = dconfig.getKeyValuePairsAt("default.foo.bat")
    assert(keyValPairs.size  == 4)
    for (pair <- keyValPairs.filter(p => p.fullPath.endsWith("first"))) {
      assert(pair.value == pair.key)
    }
  }


}
