package com.beta.Connector

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions.mapAsScalaMap

class ConfToJson(conf:String) {

  val config = ConfigFactory.load("application.conf")
  val test =config.getObject(conf).map({case (k, v) => (k, v.unwrapped())}).toMap
  val res =new MapToJson(test).res

}
