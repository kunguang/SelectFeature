package SelectFeature

import Model.selectxmldata
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhukunguang on 16/11/17.
  */

/*
    解决的问题:
            1.给定单特征,产生各种组合的交叉特征.
            2.在单特征和组合后的交叉特征中选择训练时候需要的特征.

    关键点:
          1.单特征分为3种类型,连续特征(age,历史ctr),单ID类特征(比如custuid,一条样本中custuid只能有一个),多ID类特征(用户的兴趣是多个维度)
          2.交叉特征,分为3种,单ID和单ID,单ID和多ID,多ID和多ID
          3.数据中有3种连接符:
              ","  独立特征之间的链接,比如age,sex
              "_" 交叉特征之间的链接,比如age_sex
              "|" 多ID类特征之间的链接,比如age_兴趣这个特征,会是 12_足球|12_篮球
 */


class CrossFeature extends Serializable{



  var xmldata:selectxmldata = null

  def setXmldata(xmldata:selectxmldata): Unit ={
      this.xmldata = xmldata
  }

  //在交叉之前,加入很多单特征的处理,比如age的变化,等等.做一些简单的处理,只需要注意,最后返回的结果顺序必须和输入时候的一致,并且用,链接
  def process(data:RDD[String]): RDD[String] ={

    data
  }


  def getfinaldata(data:RDD[String]): RDD[String] ={

    data.map {x =>

      var tokens:Array[String] = x.split(",")

      //存储每个交叉特征对应的值,key是crossfeature的name,value是真正交叉后产生的结果,比如key是age_sex,value是12_女
      var crossmap = Map[String,String]()

      /*
      遍历xmldata中的crossarray
       */

      var tmpcrossnames:ArrayBuffer[String] = null
      var tmpcrossnames1:ArrayBuffer[String] = null

      for(crossname <- xmldata.crossArray){
            var names:Array[String] = crossname.split("_") //crossfeature的name是用"_"连接的

            var namelength = names.length
            tmpcrossnames = ArrayBuffer[String]()
            //每次选取2个交叉.
            var crossindex1:Array[String] = tokens(xmldata.singleMap.get(names(0)).get).split("\\|")
            var crossindex2:Array[String] = tokens(xmldata.singleMap.get(names(1)).get).split("\\|")

            for(v1 <- crossindex1){
              for(v2 <- crossindex2){
                tmpcrossnames += (v1+"_"+v2)
              }
            }
            var j = 2
            while(j < namelength){
              tmpcrossnames1 = ArrayBuffer[String]()
              crossindex2 = tokens(xmldata.singleMap.get(names(j)).get).split("\\|")
              for(v1 <- tmpcrossnames){
                for(v2 <- crossindex2){
                    tmpcrossnames1 += (v1 + "_"+v2)
                }
              }

              tmpcrossnames.clear()
              for(value <- tmpcrossnames1){
                tmpcrossnames += value
              }
              j = j + 1
            }

          crossmap += (crossname -> tmpcrossnames.mkString("|"))

      }

      //做选择
      var resarray = new Array[String](xmldata.selectMap.size)

      for((k,v) <- xmldata.selectMap){
            if(crossmap.contains(k)){
              resarray(v) = crossmap.get(k).get //添加交叉特征
            }else if(xmldata.singleMap.contains(k)){
              resarray(v) = tokens(xmldata.singleMap.get(k).get) //添加本来就有的单特征
            }else{
              var a = 0
            }
      }
      resarray.mkString(",") //最后的结果用,链接
    }


  }

}
