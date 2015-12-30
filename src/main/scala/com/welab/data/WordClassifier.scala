package com.welab.data

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by welab on 15/12/7.
  */
object WordClassifier {

  def  loadClassifier(sc:SparkContext,
              filename:String):Array[String]= {

     val dict = sc.textFile(filename).
       distinct().
       collect()
     val classifier = sc.broadcast(dict)

     classifier.value

  }


  def predict(line:String,
             classifier:Array[String]):Int = {

    var C = 0
    var next = 0
    var flag = false

    while (!flag &&
      next < classifier.size) {
      val feature = classifier(next)
      if (line.indexOf(feature)>=0) {
        C = 1
        flag = true
      }
      next += 1
    }
    C
  }


  def classify(line:String,
              posClassifier:Array[String],
              negClassifier:Array[String]):Int = {


    var C = predict(line,posClassifier)
    if (1==C) {
      C = 1 - predict(line,negClassifier)
    }

    C

  }



  def  main(args:Array[String]) : Unit = {
    val posPath = args(0)
    val negPath = args(1)
    val dataPath = args(2)
    val outPath = args(3)
    val sc = new SparkContext(new SparkConf())
    val posClassifier = loadClassifier(sc,posPath)
    val negClassifier = loadClassifier(sc,negPath)

    //posClassifier.foreach(println)
   // negClassifier.foreach(println)

    sc.textFile(dataPath).
       filter(classify(_,
        posClassifier,
        negClassifier)==1).
      saveAsTextFile(outPath)
  }

}
