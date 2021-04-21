package com.beta
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.ConfigFactory
import org.json4s.DefaultFormats

import scala.collection.JavaConversions.mapAsScalaMap


object test {
  def main(args: Array[String]): Unit = {

    case class SparkR(format: String, options:Map[String,String], location:String)
    val config = ConfigFactory.load("application.conf")
    val app =config.getObject("read").map({case (k, v) => (k, v.unwrapped())}).toMap
    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val res =mapper.writeValueAsString(app)


    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats
    val Sr = parse(res).extract[SparkR]
    val testO=mapper.writeValueAsString(Sr.options)

    println(app)
    println("........................")
    println(res)
    println("*****************************")
    println(Sr.options)
    println(testO)
  }
}
