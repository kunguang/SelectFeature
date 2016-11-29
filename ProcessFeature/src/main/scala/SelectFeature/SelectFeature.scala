package SelectFeature

import Model.selectxmldata
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhukunguang on 16/11/17.
  */
class SelectFeature {

}





object SelectFeature{


  def analysisconfigfile(configpath:String):selectxmldata ={


    var xmlclass = new selectxmldata()

    var singlemap = Map[String,Int]()
    var selectmap = Map[String,Int]()
    var replacemap = Map[String,Array[String]]()

    var crossarray = ArrayBuffer[String]()

    val data =  xml.XML.loadString(configpath)

    var dropname = ArrayBuffer[String]()

    var category = ""
    var name = ""
    var index = -1
    var selectindex = -1

    var replacename:Array[String] = null

    var continuemapname = ArrayBuffer[String]()
    var needmap = -1

    for( item <- data \ "featurethreshold"){
      xmlclass.setfeaturefrequent(item.text.toInt)
    }

    for (item <- data \ "item"){
      //category 有2种可能."singlefeature","crossfeature"

      replacename = null
      for( cate <- item \"category"){
        category = cate.text
      }
      needmap = -1
      for( cate <- item \"needmap"){
        needmap = cate.text.toInt
      }

      for( n <- item \"name"){
         name = n.text
      }

      for( n <- item \"replace"){
        replacename = n.text.split(",")
      }

      for( i <- item \"index"){
         index = i.text.toInt
      }
      selectindex = -1
      for( i <- item \"selectindex"){
         selectindex = i.text.toInt
      }

      category = ""
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
      if(replacename != null){
          replacemap += (name -> replacename)
      }
      if(needmap != -1){
        continuemapname += name
      }

      if(selectindex == -1 && needmap == 1){
        dropname += name
      }

    }
    var selectmapsize = selectmap.size
    //将临时变量dropname,也加入到selectmap中.临时变量的编号排在后几位
    var dropindex = ArrayBuffer[Int]()
    for(name <- dropname){
      selectmap += (name -> selectmapsize)
      dropindex += selectmapsize
      selectmapsize = selectmapsize + 1
    }
    xmlclass.setSinglemap(singlemap)
    xmlclass.setCrossArray(crossarray.toArray)
    xmlclass.setSelectmap(selectmap)
    xmlclass.setreplacemap(replacemap)
    xmlclass.setcongtinuemapname(continuemapname.toArray)
    xmlclass.setdropindex(dropindex.toArray)
    println("continuemapname\t"+continuemapname.mkString(","))
    xmlclass

  }
  def main(args :Array[String]): Unit ={

    val conf = new SparkConf().setAppName("kunguangadfea")
      .set("spark.yarn.driver.memoryOverhead", "4000").set("spark.akka.frameSize", "128").set("spark.driver.maxResultSize", "3g")


    val sc = new SparkContext(conf)
    val inputdatapath = args(0)
    val configpath = args(1)
    val savepath = args(2)

    val configstr = sc.textFile(configpath).collect().mkString("")
    val xmldata = analysisconfigfile(configstr)

    val readdata = sc.textFile(inputdatapath)
    var processfeature = new CrossFeature()
    processfeature.setXmldata(xmldata)
    //1.对单特征进行处理和各种变换,比如处理age,将age划分到各个段.
    val firstdata =  processfeature.process(readdata)
    //2.交叉操作,并且选出最终的select
    val finaldata = processfeature.getfinaldata(firstdata)
    finaldata.saveAsTextFile(savepath)

  }
}
