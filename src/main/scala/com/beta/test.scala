package com.beta

import com.typesafe.config.ConfigFactory
import net.liftweb.json.{DefaultFormats, parse}

object test  {
  implicit val formats = DefaultFormats
  case class schema(fieldName: String, fieldType: String)
  val config = ConfigFactory.load("application.conf")
  val test =config.getAnyRef("columns").toString
  val a=test.replace("SimpleConfigList(","")
  val b=a.replace(")","")
  val json = parse(b)
  val elements = (json).children
  val aa = for (acct <- elements) yield  {
    val m = acct.extract[schema]
    s"${m.fieldName} ${m.fieldType}"
  }
  val res=aa.mkString(",")


  }