package com.welab.data

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by welab on 15/12/9.
  */
object ImplicitMatch {


  def main(args:Array[String]) : Unit = {


    val dataPath = args(0)
    val outPath = args(1)
    val sc = new SparkContext(new SparkConf())

    val data = sc.textFile(dataPath)
    val anchors = Array(
    ("存现"),
    ("入账"),
    ("汇入"),
    ("收入"),
    ("转入"),
    ("存入"),
    ("存款") )

    data.
      filter(_.contains("转账")).
      map(_.replaceAll("[0-9a-zA-Z]","")).
      filter{s=>
        var next = 0
        var flag = true
        while (flag && next < anchors.size) {
          if (s.indexOf(anchors(next))>=0) {
            flag = false
          }
          next = next + 1
        }
        flag
      }.
      map((_,1)).
      reduceByKey(_+_).
      map(s=>
      s._1 + "\t" + s._2.toString).
      saveAsTextFile(outPath)
  }
}
