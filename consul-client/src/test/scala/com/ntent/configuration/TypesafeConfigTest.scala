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
}
