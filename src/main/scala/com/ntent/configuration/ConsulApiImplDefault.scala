package com.ntent.configuration

import java.net.URL
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.config.ConfigFactory
import org.apache.http.HttpResponse
import org.apache.http.client.fluent.{Content, Request}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils

import scala.collection.JavaConversions._

/**
  * Created by vchekan on 2/11/2016.
  */
class ConsulApiImplDefault() {
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  var index: Long = 0L

  private val appSettings = ConfigFactory.load()
  private val kvUrl = new URL(new URL(appSettings.getString("dconfig.consul.url")), "v1/kv/")

  private lazy val consulQueryParams = Map(
    "seperator" -> "/",
    "recurse" -> "").
    map(p => new BasicNameValuePair(p._1, p._2)).toList

  def put(dir: String, key: String, value: String) = {
    val url = new URL(new URL(kvUrl, dir.stripMargin('/')+ "/"), key)

    Request.
      Put(url.toString).
      bodyString(if(value == null) "" else value, ContentType.APPLICATION_JSON).
      execute().returnContent()
  }

  /** Template http request. Polling and blocking requests are created on top of this one */
  private def keyRequest(configRootPath: String) = {
    new URIBuilder(new URL(kvUrl, configRootPath).toString)
  }

  def read(configRootPath: String) = {
    val builder = keyRequest(configRootPath)
    consulQueryParams.foreach(nvp => builder.addParameter(nvp.getName,nvp.getValue))
    val url = builder.build()

    val response = Request.Get(url).execute().returnResponse()
    if(response.getStatusLine.getStatusCode == 404) {
      Array[ConsulKey]()
    } else {
      val content = EntityUtils.toString(response.getEntity)
      index = response.getLastHeader("X-Consul-Index").getValue.toLong
      val keys = if (content == null || content.length == 0) Array[ConsulKey]() else mapper.readValue(content, classOf[Array[ConsulKey]])
      keys
    }
  }

  def pollingRead(configRootPath: String) = {
    val builder = keyRequest(configRootPath).
      addParameter("index", index.toString)

    consulQueryParams.foreach(nvp => builder.addParameter(nvp.getName,nvp.getValue))

    val url = builder.build()

    val response = Request.Get(url).execute().returnResponse()
    val content = EntityUtils.toString(response.getEntity)
    if(content == null || content == "") {
      null
    } else {
      val keys = mapper.readValue(content, classOf[Array[ConsulKey]])
      index = response.getLastHeader("X-Consul-Index").getValue.toLong
      keys
    }
  }
}
