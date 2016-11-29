package getfeaturehistoryctr

import Model.featurectrxmldata
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhukunguang on 16/11/22.
  */


/*

    1. 计算每天,每个特征的历史ctr,最后输出有4列,特征 曝光次数 点击次数 日期(partitionname)
 */
class computefeaturectr extends Serializable{


  var xmldata:featurectrxmldata = null //配置文件

  def setxmlclass(xmlclass:featurectrxmldata): Unit ={
    this.xmldata = xmlclass
  }


  //在交叉之前,加入很多单特征的处理,比如age的变化,等等.做一些简单的处理,只需要注意,最后返回的结果顺序必须和输入时候的一致,并且用,链接
  def process(data:RDD[String]): RDD[String] ={

    data
  }


  def getfinaldata(data:RDD[String]): RDD[String] ={

    data.map{x =>

      var tokens = x.split(",")

      //存储每个交叉特征对应的值,key是crossfeature的name,value是真正交叉后产生的结果,比如key是age_sex,value是12_女
      var crossmap = Map[String,String]()

      /*
      遍历xmldata中的crossarray
       */

      var tmpcrossnames:ArrayBuffer[String] = null
      var tmpcrossnames1:ArrayBuffer[String] = null

      for(crossname <- xmldata.crossArray){
        var names = crossname.split("_") //crossfeature的name是用"_"连接的

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

  def computectr(data:RDD[String]): RDD[String] ={

    val getfeaturepvclk = data.flatMap{x =>

      var tokens = x.split(",")
      var array = new ArrayBuffer[(String,(Int,Int))]()


      var label = tokens(xmldata.selectMap.get("label").get).toFloat.toInt
      var featurename = ""
      var tmptokens:Array[String] = null
      for((k,v) <- xmldata.selectMap){
          if(k != "label" && k != xmldata.partitionname){
                tmptokens = tokens(v).split("\\|")
                if(xmldata.partitionname != ""){
                  for(value <- tmptokens){
                    featurename = xmldata.partitionname+":"+k + ":" + value
                    array += Tuple2(featurename,Tuple2(1,label))

                  }
                }else{
                  for(value <- tmptokens){
                    featurename = k + ":" + value
                    array += Tuple2(featurename,Tuple2(1,label))

                  }
                }
          }
      }

      array.toTraversable
    }.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))

    getfeaturepvclk.map{x =>

      if(xmldata.partitionname == ""){
        x._1+","+x._2._1+","+x._2._2   //特征名字  pv clk
      }else{
        var tokens = x._1.split(":")
        var partitionname = tokens(0)
        var featurename = tokens(1)+":"+tokens(2)
        featurename+","+x._2._1+","+x._2._2 + ","+partitionname  //特征名字 pv clk dt
      }
    }

  }
}
