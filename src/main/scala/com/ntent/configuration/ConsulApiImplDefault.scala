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
  val mapper = (new ObjectMapper() with ScalaObjectMapper).registerModule(DefaultScalaModule)
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

    /*client.
      preparePut(url.toString).
      setBody(value).
      execute().get(15, TimeUnit.SECONDS)
      */
  }

  /** Template http request. Polling and blocking requests are created on top of this one */
  private def keyRequest(configRootPath: String) = {
    //client.prepareGet(new URL(kvUrl, configRootPath).toString)
    //Request.Get(new URL(kvUrl, configRootPath).toString)
    new URIBuilder(new URL(kvUrl, configRootPath).toString)
  }

  def read(configRootPath: String) = {
    val url = keyRequest(configRootPath).
      addParameters(consulQueryParams).
      build()

    Request.Get(url).execute().returnContent().asString()
    val response = Request.Get(url).execute().returnResponse()
    val content = EntityUtils.toString(response.getEntity)
    index = response.getLastHeader("X-Consul-Index").getValue.toLong
    val keys = if(content == null || content.length == 0) Array[ConsulKey]() else mapper.readValue(content, classOf[Array[ConsulKey]])
    keys

    /*val response = keyRequest(configRootPath).
      addQueryParams(consulQueryParams).
      execute().get(30, TimeUnit.SECONDS)
    val content = response.getResponseBody
    val keys = if(content == null || content.length == 0) Array[ConsulKey]() else mapper.readValue(content, classOf[Array[ConsulKey]])
    index = response.getHeader("X-Consul-Index").toLong
    keys
    */
  }

  def pollingRead(configRootPath: String) = {
    val url = keyRequest(configRootPath).
      addParameter("index", index.toString).
      addParameters(consulQueryParams).
      build()

    val response = Request.Get(url).execute().returnResponse()
    val content = EntityUtils.toString(response.getEntity)
    val keys = mapper.readValue(content, classOf[Array[ConsulKey]])
    index = response.getLastHeader("X-Consul-Index").getValue.toLong
    keys

    /*val request = keyRequest(configRootPath).
      addQueryParam("index", index.toString).
      addQueryParams(consulQueryParams)

    val response = request.execute().get()

    val content = response.getResponseBody
    val keys = mapper.readValue(content, classOf[Array[ConsulKey]])
    index = response.getHeader("X-Consul-Index").toLong
    client.close()
    keys*/
  }
}
