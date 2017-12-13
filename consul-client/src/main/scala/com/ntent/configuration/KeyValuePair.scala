package com.ntent.configuration

/**
  * Return KVP instead of Tuple2[String,String]
  * key was being used as 'path', rename key to path and add a new key field.
  */
case class KeyValuePair(fullPath:String, value:String, key:String)
