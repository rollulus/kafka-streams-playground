package com.github.rollulus.myprocessor

import collection.JavaConversions._
import java.io.FileInputStream

object Properties {
  def create(m: Map[String, _ <: AnyRef]) = {
    val ps = new java.util.Properties
    ps.putAll(m)
    ps
  }
  def union(a: java.util.Properties, b:java.util.Properties) = {
    val ps = new java.util.Properties
    ps.putAll(b)
    ps.putAll(a)
    ps
  }
  def fromFile(filename: String) = {
    val ps = new java.util.Properties()
    ps.load(new FileInputStream(filename))
    ps
  }

  implicit class MyProperties(p: java.util.Properties) {
    def union(q: java.util.Properties) = Properties.union(p, q)
  }
}

