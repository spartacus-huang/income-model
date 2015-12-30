package com.welab.data

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by welab on 15/12/12.
  */
object RepayPredictor {


  val entryPattern = """^\【.*\】|^\[.*\]|\【.*\】$|\[.*\]$"""


  val implicInPattern = "向您.*转账|给您.*转账|向你.*转账|给你.*转账" +
    "向您.*付款|给您.*付款|向你.*付款|给你.*付款"
  val implicOutPattern = "你.*向.*转账|您.*向.*转账|交易|消费|发起.*汇款"



  val explictOutPattern = """支出冲正(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|""" +
    """支出消费(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """付款合计(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """分期付款(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """已为你代付(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """代扣了(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """代扣手机费(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """代扣煤气费(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """代扣学费(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """缴费支付(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """支出收入(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """支取本金(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """支取收入(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """取款冲正(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """发起交易(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """扣款其他(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """扣款其它(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """扣款(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """您向.*转入|"""+
    """你向.*转入|"""+
    """缴费(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """支出(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """支付(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """取现(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """转出(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """缴款(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """代扣(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """取款(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """还款(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """提现(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """转取(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """支取(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """实扣(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """划出(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """现支(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """取(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """扣(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?"""




  val explictInPattern =
    """收到(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """存现(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """入账(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """汇入(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """收入(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """转入(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """存入(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """存款(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """圈提(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """圈存(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """到账(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """到账(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """退费(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """退款(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """提现(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """代发(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """转存(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?|"""+
    """收到(\(.*\))?(\[.*\])?(\（.*\）)?(\【.*\】)?""""

val moneyPattern =  """(\-|\+)?\d+(\,\d+)*(\.\d+)?"""

  val datePattern = "[0-9]{4}-[0-9]{2}-[0-9]{2}|" +
    "[0-9]{2}-[0-9]{2}-[0-9]{2}|" +
    "[0-9]{2}-[0-9]{2}|" +
    "[0-9]{1}-[0-9]{2}|" +
    "[0-9]{2}-[0-9]{1}|" +
    "[0-9]{1}-[0-9]{1}|" +
    "[0-9]{6}月|" + 
    "[0-9]{4}年[0-9]{2}月[0-9]{2}日|" +
    "[0-9]{2}月[0-9]{2}日|" +
    "[0-9]{2}月[0-9]{1}日|" +
    "[0-9]{1}月[0-9]{2}日|" +
    "[0-9]{1}月[0-9]{1}日"

  val timePattern = "[0-9]{2}:[0-9]{2}:[0-9]{2}|" +
    "[0-9]{2}:[0-9]{2}|" +
    "[0-9]{1}:[0-9]{2}|" +
    "[0-9]{2}:[0-9]{1}|" +
    "[0-9]{1}:[0-9]{1}|" +
    "[0-9]{2}时[0-9]{2}分[0-9]{2}秒|" +
    "[0-9]{2}时[0-9]{2}分|" +
    "[0-9]{1}时[0-9]{2}分|" +
    "[0-9]{2}时[0-9]{1}分|" +
    "[0-9]{1}时[0-9]{1}分"


  val sentencePattern = "。|!|！|;|；"

  val leftNegClassifier = "卡号|尾号|账号|余额|信用卡|账户|" +
    "询|手机号|四位|信用卡|送|位|笔|收费|手续费|" +
    "获得|机票|额度|客服|于|享|保单|单号|代发|" +
    "热线|赎回|净值|满|请于|抢购|可"

  val rightNegClassifier = "年|月|日|时|分|笔|次|%|个|份|点|天|退订"

  val leftPosClassifier = "金额|总额|人民币|CNY|￥|¥|RMB|人民币元|RMB元"

  val rightPosClassifier = "元|人民币|RMB|CNY|￥|¥"

  val stopWords = "为|是| :| |:|的|支付宝"





  def cutSentences(line:String):Array[String] = {

        val text = line + "。"
        val tags = sentencePattern.
          r.
          findAllIn(text).
          toArray

        var arraySize = tags.size

        val pos = new Array[Int](arraySize)
        val sentences = new StringBuilder

        var start = 0
        var next = 0

        for (elem <- tags) {
          start = text.indexOf(elem,start)
          pos(next) = start
          start = start + elem.size
          next = next + 1
       }


      start = 0
      for (elem <- pos) {
        if (elem>start) {
          sentences ++= text.substring(start,elem) + "\t"
        }
        start = elem + 1
      }
       sentences.toString().split("\t")
  }





  def predictWithOperation(line:String,
                           operationPattern:Regex):String = {

    val text = entryPattern.r.replaceAllIn(line,"")
    var operations = operationPattern.findAllIn(text)

    var start = 0
    var money = ""
    var flag = false

    while (operations.hasNext && !flag) {
      var operator = operations.next()
      start = text.indexOf(operator,start)


      val currences = predict(text.substring(start))
      if (currences.size>0) {
        flag = true
        money = currences
      }

      start += operator.size

    }

    money
  }


  def predictOperation(line:String): String = {



    var money = predictWithOperation(line,
      explictOutPattern.r)

    if (money.size>0) {
      money = "out:" + money
    }

    if (money.size==0) {
      money = predictWithOperation(line,
        explictInPattern.r)

      if (money.size>0) {
        money = "in:" + money
      }
    }
    if (money.size==0) {
      money = predictWithOperation(line,
        implicInPattern.r)

      if (money.size>0) {
        money = "in:" + money
      }
    }
    if (money.size==0) {
      money = predictWithOperation(line,
        implicOutPattern.r)
      if (money.size>0) {
        money = "out:" + money
      }
    }

    money

  }


  def searchNumerics(line:String):
                  Array[String] = {
      moneyPattern.
        r.
      findAllIn(line).
      toArray
  }


  def posNumeric(line:String):Array[Int] = {

    val numerics = searchNumerics(line)
    var next = 0
    val positions = new Array[Int](numerics.size)
    var start = 0
    for (elem <- numerics) {
      positions(next) = line.indexOf(elem,start)
      start = positions(next) + elem.size
      next += 1
    }
    positions
  }




  def getLeftContext(line:String,
                     first:Int,
                     last:Int):String = {
    var text = ""

    if (last>first+6) {
      text = line.substring(last - 6,last)
    }
    else if (last>first) {
      text = line.substring(first,last)
    }
    text
  }




  def getRightContext(line:String,
                      first:Int,
                      last:Int):
                      String = {

    var text = ""
    if (first+5<last) {
      text = line.substring(first,first+5)
    } else if (last>first) {
      text = line.substring(first,last)
    }
    text
  }



  def predictCurrence(context:String,
                      numeric:String,
                      first:Int,
                      index:Int,
                      last:Int):
                      Boolean = {

    var isNotCurrence = false
    var isCurrence = false

    val leftContext = getLeftContext(context,
                                    first,
                                    index)

    val rightContext = getRightContext(context,
                      index + numeric.size,
                      last)

    if (leftNegClassifier.
      r.
      findAllIn(leftContext).
      hasNext) {
        isNotCurrence = true
    }

    if (rightNegClassifier.
      r.
      findAllIn(rightContext).
      hasNext) {
      isNotCurrence = true
    }

    if (!isNotCurrence) {
      if (leftPosClassifier.
        r.
        findAllIn(leftContext).
        hasNext) {
        isCurrence = true
      }
      if (rightPosClassifier.
        r.
        findAllIn(rightContext).
        hasNext) {
        isCurrence = true
      }
    }

    isCurrence
  }







  def predict(line:String):String = {
    val moneyQueue = searchNumerics(line)
    val posQueue = posNumeric(line)

    val out = new StringBuilder
    var index = 0
    var next = 0
    var first = 0
    var last = 0

      for (numeric<-moneyQueue) {
        index = line.indexOf(numeric,index)


        if (next>0) {
          first = posQueue(next-1)
        } else {
          first = 0
        }
        if (next+1<posQueue.size) {
          last = posQueue(next+1)
        } else {
          last = line.size
        }

        val isCurrence = predictCurrence(line,
          numeric,
          first,
          index,
          last)


        if (isCurrence) {
          out ++= numeric + ":" + index.toString + "#"
        }

        index += numeric.size
        next += 1
      }

      out.toString()

    }



  def statUserRepay(line:Array[String]): Array[String] = {

    val array = cutSentences(line(4))

    val sb = new StringBuilder
    for (elem <- array) {
      val dl = datePattern.r.replaceAllIn(elem,"")
      val tl = timePattern.r.replaceAllIn(dl,"")
      val text = tl.replaceAll(stopWords,"")

      val res =  predictOperation(text)
      if (res.size>0) {
        sb ++=  line(0)+":" +
          res + "\t"
      }
    }

    sb.toString().split("\t")
  }




  def main(args:Array[String]): Unit = {

    val dataPath = args(0)
    val outPath = args(1)
    val sc = new SparkContext(new SparkConf())
/*
    val data = sc.
      textFile(dataPath).
      map(_.split("\t")).
      filter(_.size==5).
      map(_(4)).
      filter(_(2)=="1").
      map{ line =>

        val array = cutSentences(line)
        val sb = new StringBuilder
        for (elem <- array) {
          val dl = datePattern.r.replaceAllIn(elem,"")
          val tl = timePattern.r.replaceAllIn(dl,"")
          val text = tl.replaceAll(stopWords,"")

          val res =  predictOperation(text)
          if (res.size>0) {
            sb ++= res + "#" + line + "\t"
          }
        }
        sb.toString()
      }.filter(_.size>0).
      saveAsTextFile(outPath)

      */

    val data = sc.textFile(dataPath).
      map(_.split("\t")).
      filter(_.size==5).
      filter(_(2).toInt==1).
      map(statUserRepay(_)).
      flatMap(s=>s).
      map(_.split(":")).
      filter(_.size==4).
     cache()


    data.map(s=>{
      s(0) + "," + s(1) + "," + s(2).replace(",","")
    }).saveAsTextFile(outPath)

    /*

    val out =  data.
     filter(_(1).contains("out"))
      .map{s =>
        (s(0),s(2).replace(",","").toDouble)
      }.reduceByKey(_+_)
    //  saveAsTextFile(outPath)

    val in = data. filter(_(1).contains("in"))
      .map{s =>
        (s(0),s(2).replace(",","").toDouble)
      }.reduceByKey(_+_)

     in.fullOuterJoin(out).map {
      s =>
        val user = s._1
        var in = s._2._1
         match {
          case Some(x) => f"$x%.2f"
          case None => "0.00"
        }
        val out = s._2._2
         match {
          case Some(x) => f"$x%.2f"
          case None => "0.00"
        }

        user + "," + in + "," + out
    }.take(100).foreach(println) */
  }
}
