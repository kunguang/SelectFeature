package getfeaturehistoryctr

import Model.{selectxmldata, featurectrxmldata}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhukunguang on 16/11/22.
  */

object Getfeaturectr{


  def analysisconfigfile(configpath:String):featurectrxmldata ={


    var xmlclass = new featurectrxmldata()

    var singlemap = Map[String,Int]()
    var selectmap = Map[String,Int]()
    var crossarray = ArrayBuffer[String]()

    val data =  xml.XML.loadString(configpath)

    var category = ""
    var name = ""
    var index = -1
    var selectindex = -1


    //获取分区名字

    for( item <- data \ "partitionname"){
      xmlclass.setPartitionname(item.text)
    }

    for (item <- data \ "item"){
      //category 有2种可能."singlefeature","crossfeature"
      category = ""
      name = ""
      index = -1
      selectindex = -1

      for( cate <- item \"category"){
        category = cate.text
      }

      for( n <- item \"name"){
        name = n.text
      }
      for( i <- item \"index"){
        index = i.text.toInt
      }
      for( i <- item \"selectindex"){
        selectindex = i.text.toInt
      }

      if(category == "singlefeature"){
        singlemap += (name -> index)
      }else if(category == "crossfeature"){
        crossarray += name
      }else{
        var a = 0
      }
      if(selectindex != -1){
        selectmap += (name -> selectindex)
      }

    }
    xmlclass.setSinglemap(singlemap)
    xmlclass.setCrossArray(crossarray.toArray)
    xmlclass.setSelectmap(selectmap)
    xmlclass

  }

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("kunguangadfea")
      .set("spark.yarn.driver.memoryOverhead", "4000").set("spark.akka.frameSize", "128").set("spark.driver.maxResultSize", "3g")


    val sc = new SparkContext(conf)
    val inputdatapath = args(0)
    val configpath = args(1)
    val savepath = args(2)
    val configfile = sc.textFile(configpath).collect().mkString("")
    val xmlclass = analysisconfigfile(configfile)

    var comclass = new computefeaturectr()
    comclass.setxmlclass(xmlclass)

    val readdata = sc.textFile(inputdatapath)

    val processdata = comclass.process(readdata)
    val finaldata = comclass.getfinaldata(processdata)
    val ctrdata = comclass.computectr(finaldata)
    ctrdata.saveAsTextFile(savepath)
  }
}
