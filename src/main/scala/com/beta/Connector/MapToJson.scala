package com.beta.Connector

import com.fasterxml.jackson.databind.ObjectMapper

class MapToJson(app:Map[String,Any]) {
  import com.fasterxml.jackson.module.scala.DefaultScalaModule
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val res =  mapper.writeValueAsString(app)
}
