package com.welab.data

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by welab on 15/12/15.
  */
object RepayClassifier {

  val negTopicClassifier = "需还款|升至|备用金|总价|狂降|返现|返|降|减|奖励|即返|利息|抽取|" +
    "结欠|将|正|进行|收益|应还|领取|任意|任|消费|每满|最小|失败|满|享|" +
    "可享|尊享|享受|每月|赠|礼券|返|抵|专线|一笔|以内|达到|满足|回复|仅|豪礼|全单|" +
    "一笔|审核|流量|狂欢|双11|余额不足|套餐|月租|开通|红包|办卡|注册|月费|动态验证码|" +
    "永久性|提额|下载|激活|话费|配合|恭祝|绑定|免单|关注|密码|任性|验证码|需要|新华快讯|" +
    "失败|特惠|爆款|限额|单笔|单日|身份认证|验证|尊享|审批|取消|领券|？|中华人民共和国|" +
    "还款日|公布|敬请|惠存|糯米券|短信|审批|最低|最高|积分|兑换|新闻|快信|面试|纳税|通知|" +
    "兑换|M值|分期业务|会员|星级|评估|通过|提示|应还款|还款账号|现可用|每多|每次|预约|超过|" +
    "申请|限额|至少|校验码"

  val negAllClassifier = "正在|在付款|失败|不成功"



  val entryPattern = """^\【.*\】|^\[.*\]|\【.*\】$|\[.*\]$"""
  val sentencePattern = "。|!|！|;|；"

  val leftNegClassifier = "卡号|尾号|账号|余额|信用卡|账户|" +
    "询|手机号|四位|信用卡|送|位|笔|收费|手续费|" +
    "获得|机票|额度|客服|于|享|保单|单号|代发|热线|赎回|" +
    "净值"
  val rightNegClassifier = "年|月|日|时|分|笔|次|%|个|份|点|天|退订|超限|失败|不成功"

  val leftPosClassifier = "金额|总额|人民币|CNY|￥|¥|RMB|人民币元|RMB元"
  val rightPosClassifier = "元|人民币|RMB|CNY|￥|¥"



  val stopWords = "为|是| :| |:|的"

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


  def main(args:Array[String]): Unit = {

    val dataPath = args(0)
    val outPath = args(1)
    val sc = new SparkContext(new SparkConf())

    val data = sc.
      textFile(dataPath).
      map(_.split("\t")).
      filter(_.size == 5).
      filter(s=>entryPattern.r.findAllIn(s(4)).hasNext).
      filter{s =>
       val lines =  cutSentences(s(4))
        var flag = false

        val sb = new StringBuilder
        var allNeg = false
        for (line<-lines) {

          var isNeg = false
          val dl = datePattern.r.replaceAllIn(line, "")
          val tl = timePattern.r.replaceAllIn(dl, "")
          val text = tl.replaceAll(stopWords, "")



          if (negTopicClassifier.
            r.
            findAllIn(text).hasNext) {
            isNeg = true
          }

          if (!isNeg && predict(text).size>0) {
            sb ++= line + "\t"
            flag = true
          }

          if (negAllClassifier.
            r.
            findAllIn(text).
            hasNext) {
            allNeg = true
          }



        }

        if (allNeg) {
          flag = false
        }
        flag

      }.
      filter(_(4).size>0).
      map(_.mkString("\t")).
      saveAsTextFile(outPath)
  }

}
