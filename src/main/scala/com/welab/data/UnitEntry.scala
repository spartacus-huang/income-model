package com.welab.data

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by welab on 15/12/28.
  */
object UnitEntry {


  val unitPattern = """\[.*\]|^\【.*\】|\[.*\]$|\【.*\】$"""
  val prefixPattern = """\[|\]|\】|\【"""

  def classEntry(line:String):Boolean = {
    unitPattern.r.findAllIn(line).hasNext
  }


  def headEntry(line:String,
                startChar:Char,
                lastChar:Char):String = {
    var unit = ""
    val start =  line.indexOf(startChar,0)
    if (start==0) {
      val last = line.indexOf(lastChar,0)
      if (last>0) {
         unit = line.substring(1,last)
      }
    }
    unit
  }

  def tailEntry(line:String,
                startChar:Char,
                lastChar:Char):String = {
    var unit = ""
    val last =  line.lastIndexOf(lastChar,0)
    if (last==line.size) {
      val start = line.lastIndexOf(startChar,0)
      if (start>0) {
        unit = line.substring(start,last)
      }
    }
    unit
  }




  def extractEntry(line:String):String = {
    prefixPattern.r.replaceAllIn(line,"")
  }

  def main (args: Array[String]) {
    val posPath = args(0)
    val unitPath = args(1)

    val sc = new SparkContext(new SparkConf())

    sc.textFile(posPath).
      map(_.split("\t")).
      filter(_.size==5).
      map(s=>s(4)).
      filter(s=>classEntry(s)).
      map(s=>{
        val sb = new StringBuilder
        val s1 = headEntry(s,'[',']')
        val s2 = headEntry(s,'【','】')
        val s3 = tailEntry(s,'[',']')
        val s4 = tailEntry(s,'【','】')
        if (s1.size>0) {
          sb ++= s1 + "\t"
        }
        if (s2.size>0) {
          sb ++= s2 + "\t"
        }
        if (s3.size>0) {
          sb ++= s3 + "\t"
        }
        if (s4.size>0) {
          sb ++= s1 + "\t"
        }
        sb.toString().split("\t")
      }
      ).
      filter(_.size>0).
      flatMap(s=>s).
      distinct().
      saveAsTextFile(unitPath)

  }



}
