package com.welab.data

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap

/**
  * Created by welab on 15/12/4.
  */
object RepayCut {


  def loadEntry(sc:SparkContext,
                path:String): HashMap[String,Int] = {

    val data = sc.textFile(path)

    val dict = new HashMap[String,Int]()

    val entries = data.collect().foldLeft(dict){
      (d:HashMap[String,Int], x: String) => d += (x->0)
    }

    entries
  }


  def fetchEntry1(line:String):String = {

    var entry = ""

    val begin = line.indexOf("[")
    val endl = line.indexOf("]")

    if ((begin < endl) && (begin>=0) && (endl>0)) {

      if (begin==0 | endl==(line.size-1)) {
        entry = line.substring(begin, endl + 1)
      }

    }

    entry
  }


  def fetchEntry2(line:String):String = {

    var entry = ""

    val begin = line.indexOf("【")
    val endl = line.indexOf("】")

    if ((begin < endl) && (begin>=0) && (endl>0)) {

      if (begin==0 | endl==(line.size-1)) {
        entry = line.substring(begin, endl + 1)
      }

    }

    entry
  }



  def nearNum(line:String): Boolean = {
    val fields = line.split("\t")


    var i = 0
    var j = 0
    var k = 0

    val pattern = """^(\-|\+)?\d+(\.\d+)?$"""

    val forward = Array(("RMB"),("CNY"),("人民币"),("￥"))

    var flag = false


    for (anchor<-forward) {

       var start = line.indexOf(anchor)
       while (start>=0 && !flag) {

         var next = start + anchor.size

         if (next < line.size) {
           if (line(next)>='0' && line(next)<='9') {
             flag = true
           }



         }

         start = line.indexOf(anchor,start+1)

       }
    }

    var start = line.indexOf("元")
    while (start>=0 && !flag) {

      var prev = start -1

      if (prev >= 0) {
        if (line(prev)>='0' && line(prev)<='9') {
          flag = true
        }
      }
      start = line.indexOf("元",start+1)
    }

    flag
    }



  def main (args: Array[String]) {
    val sc = new SparkContext(new SparkConf())
    val entryPath = args(0)
    val dataPath = args(1)
    val outPath = args(2)

    val entry = sc.broadcast(loadEntry(sc,entryPath))



    val data = sc.
      textFile(dataPath).
      map(_.split("\t")).
      filter(_.size==5).
      map(_(4))

    data.filter{
      s=>
        val x=fetchEntry1(s)
        val y=fetchEntry2(s)
        entry.value.contains(x) | entry.value.contains(y)
    }.filter(nearNum(_)).
      saveAsTextFile(outPath)
  }
}