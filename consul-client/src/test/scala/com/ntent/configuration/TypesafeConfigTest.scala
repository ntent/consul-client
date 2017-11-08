package com.ntent.configuration

import com.typesafe.config.ConfigFactory
import org.scalatest._

/**
  * Created by tstumpges on 6/5/2017.
  */
class TypesafeConfigTest  extends FlatSpec with Matchers with OneInstancePerTest with BeforeAndAfterEach {
  System.setProperty("dconfig.consul.url", "")
  System.setProperty("dconfig.consul.keyStores", "")
  ConfigFactory.invalidateCaches()

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

  it should "understand time values" in {
    // Different time values to express "one day".
    System.setProperty("foo.day.ms_no_units", "86400000")
    System.setProperty("foo.day.sec_no_units", "86400")
    System.setProperty("foo.day.minutes_no_units", "1440")
    System.setProperty("foo.day.ms", "86400000ms")
    System.setProperty("foo.day.ms_plus_1", "86400001ms")
    System.setProperty("foo.day.sec", "86400s")
    System.setProperty("foo.day.min", "1440m")
    System.setProperty("foo.day.hours", "24h")
    System.setProperty("foo.day.days", "1d")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings

    assert(dconfig.getMs("foo.day.ms_no_units") == 86400000)
    assert(dconfig.getMs("foo.day.ms")          == 86400000)
    assert(dconfig.getMs("foo.day.ms_plus_1")   == 86400000 + 1)
    assert(dconfig.getMs("foo.day.sec")         == 86400000)
    assert(dconfig.getMs("foo.day.min")         == 86400000)
    assert(dconfig.getMs("foo.day.hours")       == 86400000)
    assert(dconfig.getMs("foo.day.days")        == 86400000)

    assert(dconfig.getSec("foo.day.sec_no_units") == 86400)
    assert(dconfig.getSec("foo.day.ms")           == 86400)
    assert(dconfig.getSec("foo.day.ms_plus_1")    == 86400 + 1)
    assert(dconfig.getSec("foo.day.sec")          == 86400)
    assert(dconfig.getSec("foo.day.min")          == 86400)
    assert(dconfig.getSec("foo.day.hours")        == 86400)
    assert(dconfig.getSec("foo.day.days")         == 86400)

    assert(dconfig.getMinutes("foo.day.minutes_no_units") == 1440)
    assert(dconfig.getMinutes("foo.day.ms")               == 1440)
    assert(dconfig.getMinutes("foo.day.ms_plus_1")        == 1440 + 1)
    assert(dconfig.getMinutes("foo.day.sec")              == 1440)
    assert(dconfig.getMinutes("foo.day.min")              == 1440)
    assert(dconfig.getMinutes("foo.day.hours")            == 1440)
    assert(dconfig.getMinutes("foo.day.days")             == 1440)
  }

  it should "understand size values" in {
    // Different time values to express "one gigabyte".
    System.setProperty("foo.gig.b_no_units", s"${8L * 1024 * 1024 * 1024}")
    System.setProperty("foo.gig.kb_no_units", s"${8 * 1024 * 1024}")
    System.setProperty("foo.gig.mb_no_units", s"${8 * 1024}")
    System.setProperty("foo.gig.gb_no_units", "8")
    System.setProperty("foo.gig.b", s"${8L * 1024 * 1024 * 1024}b")
    System.setProperty("foo.gig.b_plus_1", s"${8L * 1024 * 1024 * 1024 + 1}B")
    System.setProperty("foo.gig.k", s"${8 * 1024 * 1024}k")
    System.setProperty("foo.gig.kb", s"${8 * 1024 * 1024}KB")
    System.setProperty("foo.gig.m", s"${8 * 1024}m")
    System.setProperty("foo.gig.mb", s"${8 * 1024}MB")
    System.setProperty("foo.gig.g", "8g")
    System.setProperty("foo.gig.gb", "8GB")
    ConfigFactory.invalidateCaches()
    val dconfig = new TypesafeConfigSettings

    assert(dconfig.getBytes("foo.gig.b_no_units") == 8L * 1024 * 1024 * 1024)
    assert(dconfig.getBytes("foo.gig.b")          == 8L * 1024 * 1024 * 1024)
    assert(dconfig.getBytes("foo.gig.b_plus_1")   == 8L * 1024 * 1024 * 1024 + 1)
    assert(dconfig.getBytes("foo.gig.k")          == 8L * 1024 * 1024 * 1024)
    assert(dconfig.getBytes("foo.gig.kb")         == 8L * 1024 * 1024 * 1024)
    assert(dconfig.getBytes("foo.gig.m")          == 8L * 1024 * 1024 * 1024)
    assert(dconfig.getBytes("foo.gig.mb")         == 8L * 1024 * 1024 * 1024)
    assert(dconfig.getBytes("foo.gig.g")          == 8L * 1024 * 1024 * 1024)
    assert(dconfig.getBytes("foo.gig.gb")         == 8L * 1024 * 1024 * 1024)

    assert(dconfig.getKb("foo.gig.kb_no_units") == 8 * 1024 * 1024)
    assert(dconfig.getKb("foo.gig.b")           == 8 * 1024 * 1024)
    assert(dconfig.getKb("foo.gig.b_plus_1")    == 8 * 1024 * 1024 + 1)
    assert(dconfig.getKb("foo.gig.k")           == 8 * 1024 * 1024)
    assert(dconfig.getKb("foo.gig.kb")          == 8 * 1024 * 1024)
    assert(dconfig.getKb("foo.gig.m")           == 8 * 1024 * 1024)
    assert(dconfig.getKb("foo.gig.mb")          == 8 * 1024 * 1024)
    assert(dconfig.getKb("foo.gig.g")           == 8 * 1024 * 1024)
    assert(dconfig.getKb("foo.gig.gb")          == 8 * 1024 * 1024)

    assert(dconfig.getMb("foo.gig.mb_no_units") == 8 * 1024)
    assert(dconfig.getMb("foo.gig.b")           == 8 * 1024)
    assert(dconfig.getMb("foo.gig.b_plus_1")    == 8 * 1024 + 1)
    assert(dconfig.getMb("foo.gig.k")           == 8 * 1024)
    assert(dconfig.getMb("foo.gig.kb")          == 8 * 1024)
    assert(dconfig.getMb("foo.gig.m")           == 8 * 1024)
    assert(dconfig.getMb("foo.gig.mb")          == 8 * 1024)
    assert(dconfig.getMb("foo.gig.g")           == 8 * 1024)
    assert(dconfig.getMb("foo.gig.gb")          == 8 * 1024)

    assert(dconfig.getGb("foo.gig.gb_no_units") == 8)
    assert(dconfig.getGb("foo.gig.b")           == 8)
    assert(dconfig.getGb("foo.gig.b_plus_1")    == 8 + 1)
    assert(dconfig.getGb("foo.gig.k")           == 8)
    assert(dconfig.getGb("foo.gig.m")           == 8)
    assert(dconfig.getGb("foo.gig.mb")          == 8)
    assert(dconfig.getGb("foo.gig.g")           == 8)
    assert(dconfig.getGb("foo.gig.gb")          == 8)
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
    for (pair <- keyValPairs.filter(p => p.key.endsWith("first"))) {
      assert(pair.value.equals(pair.key))
    }
  }
}
