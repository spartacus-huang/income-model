package com.welab.data

import org.apache.spark.SparkContext

/**
  * Created by welab on 15/11/26.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



object Repayment {




  def getEnd(line:String,
             pos:Int): Int = {


    var next = pos + 2
    var stop:Boolean = false
    var endl = line.size - 1

    while ((next<line.size)
      && !stop) {
     if ( line(next) == '.' ) {
        val predict = next + 1
        if (predict<line.size) {
          if ((line(predict)<'0')
            | (line(predict)>'9') ) {
            stop = true
            endl = next
          }
        } else {
          stop = true
          endl = next
        }
     }

     else if ( line(next) == ',' ) {
        val predict = next + 1
        if (predict<line.size) {
          if ((line(predict)<'0')
            | (line(predict)>'9') ) {
            stop = true
            endl = next
          }
        } else {
          stop = true
          endl = next
        }
      }
     else if ( line(next) == '，' ) {
       val predict = next + 1
       if (predict<line.size) {
         if ((line(predict)<'0')
           | (line(predict)>'9') ) {
           stop = true
           endl = next
         }
       } else {
         stop = true
         endl = next
       }
     }

     else if ( line(next) == '元' ) {
          stop = true
          endl = next
        }
     else if ( line(next) == '。' ) {
       stop = true
       endl = next
     }


      next = next + 1
    }

    endl

  }



  //val anchors = Array(("支出"),("支出冲正"),("支出人民币"),
  //("支出￥"),"")
  //

















  def forwardMoney(line:String,
                  start:Int,
                  endl:Int):String = {

    var next = start
    var first = -1
    var last = line.size

    while (next<endl && last==line.size) {

      if ((line(next)>='0')&&
        (line(next)<='9')) {
        if (first<0) {
          first = next
        }
      } else if (line(next)=='.') {

      } else if (line(next)==',') {

      } else if (line(next)=='-') {

      } else if (first>0) {
        last = next
      }

      next = next + 1

    }

    var  money=""
    if (first>0 && last<line.size) {
      money = line.substring(first,last)
    }

    money
  }

  def getMoney(line:String,
              start:Int,
              endl:Int): String = {

       var next = endl

       var first = -1
       var last = line.size

       while ((next >= start) && (first < 0)) {

          if ((line(next)>='0')&&
            (line(next)<='9')) {
            if (last==line.size) {
              last = next
            }
          } else if (line(next)=='.') {

          } else if (line(next)==',') {

          } else if (line(next)=='-') {

          } else if (last < line.size) {
            first = next + 1
          }

         next = next - 1
       }

    var  money=""
    if (first>0 && last<line.size) {
      money = line.substring(first,last+1)
    }

    money
  }




  def main(args:Array[String]): Unit = {

        val dataPath = args(0)
        val outPath = args(1)
        val sc = new SparkContext(new SparkConf())

        val data = sc.textFile(dataPath)

    /*    data.filter(_.contains("支出")).
            map{ s=>
               val action =  s.indexOf("支出")
               val start = action + 2


               val endl = getEnd(s,start)
               val money = getMoney(s,start,endl)
               val index = s.indexOf(money)

               var cat = ""
               if ((index > start) && (index < endl)) {
                  cat = s.substring(start,index-1)
               }


                "支出"+"\t" + money + "\t" + s(endl) + "\t" +s

            }.saveAsTextFile(outPath) */

/*
     data.
       filter(_.contains("银行】")).
       filter(_.contains("取出")).
       map {
       s =>
         val action =  s.indexOf("取出")
         val start = action + 2

         val money = forwardMoney(s,start,s.length)
         val index = s.indexOf(money)
         var cat = ""
         if ((index > start) && (index < s.length)) {
           cat = s.substring(start,index-1)
         }


         "取出"+"\t" + money + "\t" + cat + "\t" +s

       }.saveAsTextFile(outPath)
*/

/*
    data.
      filter(_.contains("银行】")).
      filter(!_.contains("圈提转入")).
      filter(_.contains("圈提")).
      map {
        s =>
          val action =  s.indexOf("圈提")
          val start = action + 2

          val money = forwardMoney(s,start,s.length)
          val index = s.indexOf(money)
          var cat = ""
          if ((index > start) && (index < s.length)) {
            cat = s.substring(start,index-1)
          }


          "圈提"+"\t" + money + "\t" + cat + "\t" +s

      }.saveAsTextFile(outPath)
**/
    /*
    data.
      filter(_.contains("中国邮政】")).
      filter(_.contains("汇出金额")).
      map{
        s =>
          val action =  s.indexOf("汇出金额")
          val start = action + 4

          val money = forwardMoney(s,start,s.length)
          val index = s.indexOf(money)
          var cat = ""
          if ((index > start) && (index < s.length)) {
            cat = s.substring(start,index-1)
          }


          "汇出金额"+"\t" + money + "\t" + cat + "\t" +s
    }.saveAsTextFile(outPath)

*/

    /*
    data.
      filter(_.contains("中国银行】")).
      filter(_.contains("汇款")).
        map{
          s =>
            val action =  s.indexOf("人民币元")
            val start = action + 4
            val money = forwardMoney(s,start,s.length)
            val index = s.indexOf(money)
            var cat = ""
            if ((index > start) && (index < s.length)) {
              cat = s.substring(start,index-1)
            }


            "汇款金额"+"\t" + money + "\t" + cat + "\t" +s
        }.saveAsTextFile(outPath)

        */


    data.
      filter(_.contains("扣款")).
      filter(!_.contains("将于")).
      filter(!_.contains("应扣")).
      filter(!_.contains("督促")).
      map{
        s =>
          val action =  s.indexOf("扣款")
          val start = action + 4
          val money = forwardMoney(s,start,s.length)
          val index = s.indexOf(money)
          var cat = ""
          if ((index > start) && (index < s.length)) {
            cat = s.substring(start,index-1)
          }


          "扣款"+"\t" + money + "\t" + cat + "\t" +s
      }.saveAsTextFile(outPath)
  }



}
