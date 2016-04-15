package com.ntent.configuration

import java.net.URL
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.ning.http.client.{AsyncHttpClient, Param}
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by vchekan on 2/11/2016.
  */
class ConsulApiImplDefault() {
  val mapper = (new ObjectMapper() with ScalaObjectMapper).registerModule(DefaultScalaModule)
  var index: Long = 0L

  private val appSettings = ConfigFactory.load()
  private val dc = appSettings.getString("dconfig.consul.dc")
  private val kvUrl = new URL(new URL(appSettings.getString("dconfig.consul.url")), "v1/kv/")

  //lazy val configRootPath = appSettings.getString("dconfig.consul.configRoot").stripMargin('/') + '/'
  private lazy val consulQueryParams = Map(
    "seperator" -> "/",
    "dc" -> dc,
    "recurse" -> "").
    map(p => new Param(p._1, p._2)).toList

  def put(dir: String, key: String, value: String) = {
    val url = new URL(new URL(kvUrl, dir.stripMargin('/')+ "/"), key)

    new AsyncHttpClient().
      preparePut(url.toString).
      addQueryParam("dc", dc).
      setBody(value).
      execute().get(15, TimeUnit.SECONDS)
  }

  /** Template http request. Polling and blocking requests are created on top of this one */
  private def keyRequest(configRootPath: String) = {
    val http = new AsyncHttpClient
    http.prepareGet(new URL(kvUrl, configRootPath).toString)
  }

  def read(configRootPath: String) = {
    val response = keyRequest(configRootPath).
      addQueryParams(consulQueryParams).
      execute().get(30, TimeUnit.SECONDS)
    val content = response.getResponseBody
    val keys = if(content == null || content.length == 0) Array[ConsulKey]() else mapper.readValue(content, classOf[Array[ConsulKey]])
    index = response.getHeader("X-Consul-Index").toLong
    keys
  }

  def pollingRead(configRootPath: String) = {
      val response = keyRequest(configRootPath).
        addQueryParam("index", index.toString).
        addQueryParams(consulQueryParams).
        execute().

        get()
        val content = response.getResponseBody
        val keys = mapper.readValue(content, classOf[Array[ConsulKey]])
        index = response.getHeader("X-Consul-Index").toLong
        keys
  }
}
