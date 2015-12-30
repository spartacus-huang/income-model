package com.welab.data

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by welab on 15/12/6.
  */
object RepayExtracor {


  def isNumber(ch:Char):Boolean = {
    ((ch>='0') && (ch<='9'))
  }


  def predictMoney(line:String,
                   term:String):String = {

    val moneyPattern =  """(\-|\+)?\d+(\,\d+)*(\.\d+)?""".r
    val moneyClassifier = """金额|总额|人民币|CNY|￥|¥|RMB|元""".r
    val leftIndex = line.indexOf(term)
    var money = ""

    val leftLine = line.substring(0,leftIndex)
    val leftLeafs = moneyPattern.
            findAllIn(leftLine).
            toArray

    if (leftLeafs.size>0) {
      val leftLeaf = leftLeafs(leftLeafs.size - 1)
      val leftPos = leftLine.lastIndexOf(leftLeaf)
      var leftStart = leftPos - 5
      if (leftStart < 0) {
        leftStart = 0
      }
      var leftEnd = leftPos + leftLeaf.size + 5

      if (leftEnd > leftLine.size ) {
        leftEnd = leftLine.size
      }

      val leftSample = leftLine.substring(leftStart, leftEnd)

      if (moneyClassifier.findAllIn(leftSample).toArray.size > 0) {
        money = leftLeaf
      }

    }
      val rightIndex = leftIndex + "转账".size
      val rightLine = line.substring(rightIndex)
      val rightLeafs = moneyPattern.
        findAllIn(rightLine).
        toArray

      if (rightLeafs.size>0) {
        val rightLeaf = rightLeafs(0)
        val rightPos = rightLine.indexOf(rightLeaf)
        var rightStart = rightPos - 5
        if (rightStart<0) {
          rightStart = 0
        }
        var rightEnd = rightPos + rightLeaf.size + 5

        if (rightEnd > rightLine.size) {
          rightEnd = rightLine.size
        }

        val rightSample = rightLine.substring(rightStart,rightEnd)

        if (moneyClassifier.findAllIn(rightSample).toArray.size>0) {
          money = rightLeaf
        }
      }
    money
  }


  def predictDirection(line:String):String = {

    val inPattern = "向您.*转账|给您.*转账".r
    val outPattern = "您.*向.*转账|您.*给.*转账|您.*经.*转账".r
    if (inPattern.findAllIn(line).toArray.size>0) {
      "in"
    } else if (outPattern.findAllIn(line).toArray.size>0) {
      "out"
    } else {
      "none"
    }
  }

  def getMoneyFeature():String  = {

    val features = "(金额|总额)?(人民币|CNY|￥|¥|RMB)?元?"
    val prefix = """为?是?:?：?\)?\）?\]?\】?"""
    val money =  """(\-|\+)?\d+(\,\d+)*(\.\d+)?$"""
    val suffix = """\(?\（?\[?\【?元?"""
    features.concat(prefix).concat(money).concat(suffix)

  }






  def implicitOutMoney(line:String) : String = {

    val moneyValue = """^(\\-|\\+)?\\d+(\\,\\d+)*(\\.\\d+)?$"""
    val moneyPrefix = """[金额][金额：][金额:][总额][总额：][总额:]"""
    """[人民币][人民币：][人民币:][CNY][¥][￥][RMB][元][：][:]"""
    val moneySuffix = """[人民币][CNY][¥][￥][RMB][元]"""

    val moneyPattern = (moneyPrefix + moneyValue + moneySuffix).r

    val features = "您.*向.*转账[,][，]".r

    var money = ""

    val predict = features.findAllIn(line).toArray

    for (elem <- predict) {

      val index = elem.indexOf("发起")

      if(index>=0) {
          val moneys = moneyPattern.findAllIn(elem.substring(index)).toArray
          if (moneys.size>0) {
            money = moneys(0)
          }
      } else {

        val pos = line.indexOf(elem) + elem.size
        val money1 = moneyPattern.findAllIn(line.substring(pos)).toArray
        if (money1.size>0) {
          val valid = line.indexOf(money1(0))
          if (valid==pos) {
            money = money1(0)
          }
        }
      }
    }
    money
  }


  def implicitInMoney(line:String) : String = {

    val moneyValue = """^(\\-|\\+)?\\d+(\\,\\d+)*(\\.\\d+)?$"""
    val moneyPrefix = """[金额][总额][人民币][CNY][¥][￥][RMB][元][：][:]"""
    val moneySuffix = """[人民币][CNY][¥][￥][RMB][元]"""

    val moneyPattern = (moneyPrefix + moneyValue + moneySuffix).r

    val features = "向您.*转账[,][，]".r

    var money = ""

    val predict = features.findAllIn(line).toArray

    for (elem <- predict) {

      val index = elem.indexOf("发起")

      if(index>=0) {
        val moneys = moneyPattern.findAllIn(elem.substring(index)).toArray
        if (moneys.size>0) {
          money = moneys(0)
        }
      } else {

        val pos = line.indexOf(elem) + elem.size
        val money1 = moneyPattern.findAllIn(line.substring(pos)).toArray
        if (money1.size>0) {
          val valid = line.indexOf(money1(0))
          if (valid==pos) {
            money = money1(0)
          }
        }
      }
    }
    money
  }

  def implicitExtractMoney(line:String,
                           anchors:Array[String])
  :String = {


    val moneyPattern = """^(\\-|\\+)?\\d+(\\,\\d+)*(\\.\\d+)?$""".r

    val feature1 = "向您.*转账，金额为".r
    val feature2 = "向您.*发起.*转账".r
    val feature3 = "您.*向.*转账".r
    var money = ""

    val s = feature1.findAllIn(line).toArray

    if (s.size>0) {

      val start = line.indexOf(s(0)) + s(0).size

      val moneys = moneyPattern.findAllIn(
        line.substring(start)).
        toArray

      if (moneys.size>0) {
         if (line.indexOf(moneys(0))==start) {
           money = moneys(0)
         }
      }
    }

    if (money.size==0) {
      val s1 = feature2.findAllIn(line).toArray
      if (s1.size>0) {
        val s2 = feature2.findAllIn(s1(0).substring(s1(0).indexOf("发起"))).toArray
        if (s2.size > 0) {
          money = s2(0)
        }
      }
    }

    if (money.size==0) {
      val s3 = feature3.findAllIn(line).toArray
      if (s3.size>0) {

      }
    }

    money
  }

  def explicitExtractMoney(line:String,
                      anchors:Array[String])
                      :String = {
    var flag = false
    var next = 0
    var start = 0
    var money = ""
    var tag = 0

    while (!flag &&
      next < anchors.size) {

      start = -1
      tag = -1
      start = findAnchor(line, anchors(next), 0)
      if(start>=0) {
        tag = tagMoney(line, start)
      }

      if (tag>=0) {
        money = findMoney(line, tag)
      }


      while (money.size == 0 &&
        tag >= 0) {
        start = findAnchor(line, anchors(next), tag + 1)
        if(start>=0) {
          tag = tagMoney(line, start)
        } else {
          tag = start
        }
        if (tag >= 0) {
          money = findMoney(line, tag)
        }
      }

      if (money.size>0) {
        flag = true
      }
      next = next + 1
    }

    if (money.size>0) {
      money =  anchors(next-1) + "\t" + money
    }

    money

  }

  def extractRepay(line:String):String = {


    val explicitOutAnchors = Array(
      ("支出冲正"),
      ("支出消费"),
      ("付款合计"),
      ("分期付款"),
      ("已为你代付"),
      ("代扣了"),
      ("代扣手机费"),
      ("代扣煤气费"),
      ("代扣学费"),
      ("缴费支付"),
      ("支出收入"),
      ("支取本金"),
      ("支取收入"),
      ("取款冲正"),
      ("发起交易"),
      ("缴费"),
      ("支出"),
      ("支付"),
      ("取现"),
      ("转出"),
      ("付款"),
      ("缴款"),
      ("代扣"),
      ("取款"),
      ("提现"),
      ("转取"),
      ("支取"),
      ("实扣"),
      ("划出"),
      ("代付"),
      ("收到")
    )


    val explicitInAnchors = Array(
      ("存现"),
      ("入账"),
      ("汇入"),
      ("收入"),
      ("转入"),
      ("存入"),
      ("存款"),
      ("收到")
    )

    val implicitAnchors = Array(
      ("支出业务"),
      ("消费交易"),
      ("支付交易"),
      ("手续费"),
      ("代付"),
      ("还款"),
      ("消费"),
      ("转账")
    )

  var  money =  explicitExtractMoney(line,
    explicitOutAnchors)

  if (money.size == 0) {
    money = explicitExtractMoney(line,
    explicitInAnchors)
  }

  if (money.size ==0) {
    money = implicitInMoney(line)
  }

    if (money.size==0) {
      money = predictDirection(line)
      money = money + "\t" + predictMoney(line,"转账")
      money = money + "\t" + line
    }

    
    money
  }


  def findAnchor(line:String,
                 term:String,
                 start:Int): Int = {
    var index = line.indexOf(term,start)

    if (index>=0) {
      var next = index + term.size

      while (next<line.size &&
        line(next)==' ') {
        next += 1
      }


      if (next < line.size &&
        line(next)=='(') {
        next = next + 1
        while(next<line.size &&
          line(next)!=')') {
          next += 1
        }
        next = next + 1
      }


      if (next < line.size &&
        line(next)=='（') {
        next = next + 1
        while(next<line.size &&
          line(next)!='）') {
          next += 1
        }
        next = next + 1
      }

      if (next < line.size &&
        line(next)=='[') {
        next = next + 1
        while(next<line.size &&
          line(next)!=']') {
          next += 1
        }
        next = next + 1
      }

      if (next < line.size &&
        line(next)=='【') {
        next = next + 1
        while(next<line.size &&
          line(next)!='】') {
          next += 1
        }
        next = next + 1
      }


      while (next<line.size &&
        line(next)==' ') {
        next += 1
      }

      index = next
    }

    index
  }


  def tagMoney(line:String,
              index:Int): Int = {
    val tags = Array(("金额¥"),("金额￥"),
      ("人名币"),("金额CNY"),
      ("rmb"),("金额为"),
      ("金额RMB"), ("金额人民币"),
      ("金额:"),("金额："),
      ("支付宝"),("金额"),
      ("RMB"),("人民币"),
      ("CNY"),("¥"),
      ("￥"),("现金"),
      ("总额:"),("总额："),
      ("总额"),("为"),
      ("为:"),("为："))

    var next = 0
    var flag = false
    var start = index

    while (!flag && (start < line.size)) {
      if (line(start)==' ') {
        start += 1
      } else {
        flag = true
      }
    }

    flag = false


    while (!flag && next < tags.size) {

      var endl = start + tags(next).size

      if (endl<=line.size) {
        val atag = line.substring(start,endl)
        if (tags(next) == atag) {
          start = start + tags(next).size
          flag = true
        }
      }

      next = next + 1
    }


    flag = false
    while (!flag && (start < line.size)) {
      if (line(start)==' ') {
        start += 1
      } else if (line(start)==':') {
        start += 1
      } else if (line(start)=='：') {
        start += 1
      }  else {
        flag = true
      }
    }

    start

  }


  def findMoney(line:String,
                index:Int):String = {

    var money = new StringBuilder
    var flag = false
    var next = index


    if (next<line.size &&
      (line(next)=='-'
      | line(next)=='+')) {
      next = next + 1

      if (next < line.size &&
        isNumber(line(next))) {
        flag = true
        money += line(next-1)
        money += line(next)
        next = next + 1
      }

    } else if (next < line.size &&
      isNumber(line(next))) {
      flag = true
      money += line(next)
      next = next + 1
    }


    while(flag && next < line.size) {

      if (isNumber(line(next))) {
        money += line(next)
      }
      else if (line(next)==',') {
        money += line(next)
      }
      else if (line(next)=='.') {
        money += line(next)
      } else {
        flag = false
      }
      next = next + 1
    }



    money.toString()
  }


  def main(args:Array[String]): Unit = {

    val dataPath = args(0)
    val outPath = args(1)
    val sc = new SparkContext(new SparkConf())

    val data = sc.textFile(dataPath)


  //  val anchors = Array(("支出业务,"),("支出冲正"),
  //    ("[ATM支出]"), ("(支出)"),
  //    ("支出消费"),("支出收入"),
  //    ("支出"),("划出"))

   // val anchors = Array(("划出"))

 //   val anchors = Array(("【微信支付】"),("网上支付"),
  //    ("支付，"),("【新浪支付】"),("【天翼支付】"),
  //    ("【支付随心】"),("支付交易"),("【手机支付】"),
  //    ("(货款支付)"),("【艾米支付】"),("(支付宝支付)"),
   //   ("支付"))

  //  val anchors = Array(("付款合计"),("付款"))

 //   val anchors = Array(("成功消费"),("发生消费,"),
 //     ("发生一笔消费,"),("发生网银消费,"),
  //    ("消费，"),("消费,"),("消费"))

 /*   val anchors = Array(("存现"),("入账"),("汇入"),
      ("收入"),("转入"),("存入"),("存款"))
    data.filter{s =>


      var next = 0
      var flag = false
      while (!flag && next < anchors.size) {
        if (s.indexOf(anchors(next))>=0) {
          flag = true
        }
        next = next + 1
      }
      flag
    }.filter(extractRepay(_).size>0).take(1000).foreach(println) */


    data.filter(_.contains("转账")).
      filter(extractRepay(_).size>0).
     map(extractRepay(_)).
     saveAsTextFile(outPath)


  }



}
