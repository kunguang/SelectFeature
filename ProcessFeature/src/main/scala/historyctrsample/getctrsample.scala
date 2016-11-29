package historyctrsample

import Model.selectxmldata
import SelectFeature.CrossFeature
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._


/**
  * Created by zhukunguang on 16/11/22.
  */
class getctrsample extends Serializable{


  var xmldata:selectxmldata = null

  def setxmldata(xmldata:selectxmldata){

    this.xmldata = xmldata
  }

  var historyctrmap = new HashMap[String,Double]()

  def processdata(data:RDD[String]): RDD[String] ={
      data
  }
  def getfinaldata(data:RDD[String]):RDD[String] ={

    var crossfea = new CrossFeature
    crossfea.setXmldata(xmldata)
    val finaldata = crossfea.getfinaldata(data)
   // println(finaldata.take(1)(0))
    finaldata.map{x =>

      var tokens = x.split(",")
      var array = new Array[String](xmldata.selectMap.size)
      var featurename = ""
      var judgenot = 0 //判断该样本有没有缺失值
      var tmptokens:Array[String] = null
      var tmpres = ArrayBuffer[String]()
      for((k,v) <- xmldata.selectMap){
          if(k != "label" && xmldata.congtinuemapname.contains(k)) {
            tmptokens = tokens(v).split("\\|")

            for(value <- tmptokens){
              featurename = k+":"+value
              if(historyctrmap.contains(featurename)){
                tmpres += (historyctrmap.get(featurename).get + "")
                //array(v) = tmpres
              }
            }
            array(v) = tmpres.mkString("|")
          }else{
            array(v) = tokens(v)
          }
        tmpres.clear()
      }

      var replaceindexs:Array[String] = null
     var replaceindex = 0
      var queshi = 0
      //如果有缺失,先找替换
      breakable{
        for((k,v) <- xmldata.selectMap){
          if(array(v) == "" && xmldata.congtinuemapname.contains(k) && !xmldata.dropindex.contains(v)){
            if(xmldata.replaceMap.contains(k)){
                replaceindexs = xmldata.replaceMap.get(k).get
                for(i <- replaceindexs){
                  replaceindex = xmldata.selectMap.get(i).get
                  if(array(v) != "" && array(replaceindex) != ""){
                    array(v) = array(replaceindex)
                  }
                }
              if(array(v) == ""){
                queshi = 1
                break()
              }

            }else{
              queshi = 1
              break()
            }
            queshi = 1
            break()
          }
        }
      }
      if(queshi == 1){
        ""
      }else{
        //后面dropindex.size个都是存放的临时变量,去掉
        var effectsize = xmldata.selectMap.size-xmldata.dropindex.size
        var finalarray = new Array[String](effectsize)
        for(i <- 0 until effectsize){
            finalarray(i) = array(i)
        }
        finalarray.mkString(",")
      }

    }.filter(x => x != "")

  }

  //data有3列,特征 pv clk 用,链接
  def getctrmap(data:RDD[String]): Unit ={

    val featurectrarr = data.map{x =>
        var tokens = x.split(",")
        var featuredaihao = tokens(0).split(":")(0)
        if(xmldata.congtinuemapname.contains(featuredaihao)){
          (tokens(0),Tuple2(tokens(1).toInt,tokens(2).toInt))
        }else(
          ("",Tuple2(0,0))
          )
    }.filter(x => x._1 != "").reduceByKey((x,y) => (x._1+y._1,x._2+y._2)).filter(x=>x._2._1 >= xmldata.featurefrequent).collect()

    for(i <- 0 until featurectrarr.length){
          historyctrmap.put(featurectrarr(i)._1,(featurectrarr(i)._2._2 *100000/ (featurectrarr(i)._2._1+1.0+featurectrarr(i)._2._2)).toInt)
    }
    println("historyctrmapsize\t"+historyctrmap.size)
  }
}
