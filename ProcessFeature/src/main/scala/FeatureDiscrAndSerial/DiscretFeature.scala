package FeatureDiscrAndSerial

import Model.{selectxmldata, discxmldata}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhukunguang on 16/11/17.
  */



object DiscretFeature{

  def analysisconfigfile(configpath:String):discxmldata ={


    var xmlclass = new discxmldata()

    var singlemap = Map[String,Int]()
    var selectmap = Map[String,Int]()
    var tmpcontinuefeatmap = Map[String,Int]()

    var selectarray = ArrayBuffer[String]() //存储连续特征name
    val data =  xml.XML.loadString(configpath)

    var category = "" //要么是连续特征,要么是ID特征
    var name = ""
    var index = -1
    var isselect = -1 //是否被选择为训练特征
    var blocksize = 100 //连续特征离散块数


    //获取特征阈值,低于这个频次的特征丢掉

    for( item <- data \ "featurethreshold"){
          xmlclass.setfeaturefrequent(item.text.toInt)
    }
    println("featurefrequent\t"+xmlclass.featurefrequent)

    //获取是否丢掉样本,因为某些特征被频次过滤后,必然会导致某些样本缺失特征,此时就用一个统一的特征编号.这个属性决定当样本缺失某个
    //编号特征后,是否丢弃该样本,如果是1就丢弃

    for( item <- data \ "discardsample"){
      xmlclass.setdiscardsample(item.text.toInt)

    }
    println("discardsample\t"+xmlclass.isdiscardsample)
    //获取其他属性

    for (item <- data \ "item"){
      isselect = -1
      index = -1
      category = ""
      name = ""
      for( cate <- item \"category"){
        category = cate.text
      }

      for( n <- item \"name"){
        name = n.text
      }
      for( i <- item \"index"){
        index = i.text.toInt
      }
      for( index <- item \"select"){
        isselect = index.text.toInt
      }
      for( bs <- item \"blocksize"){
        blocksize = bs.text.toInt
      }
      singlemap += (name -> index)

      if(category == "continuefeature"){
        tmpcontinuefeatmap += (name -> blocksize)
      }
      if(isselect != -1){
        selectarray += name
      }

    }

    //重新编号,形成新的数据的编号顺序,构造selectmap,label排在第一位
    var startindex = 0
    if(singlemap.contains("label")){
        selectmap += ("label" -> startindex)
        startindex = startindex + 1
    }

    for((k,v) <- singlemap){
        if(k != "label" && selectarray.contains(k)){
              selectmap += (k -> startindex)
              startindex = startindex + 1
        }
    }

    //根据上面最新的编号,构建featmap
    var continuefeatmap = Map[Int,Int]()

    for((k,v) <- tmpcontinuefeatmap){
        if(selectmap.contains(k)){
            continuefeatmap += (selectmap.get(k).get -> v)
        }
    }

    var xmldata = new discxmldata()
    xmldata.setSinglemap(singlemap)
    xmldata.setContinuemap(continuefeatmap)
    xmldata.setSelectmap(selectmap)
    xmldata
  }

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("kunguangadfea")
      .set("spark.yarn.driver.memoryOverhead", "4000").set("spark.akka.frameSize", "128").set("spark.driver.maxResultSize", "3g")


    val sc = new SparkContext(conf)
    val inputdatapath = args(0)
    val configpath = args(1)
    val savetrainpath = args(2)
    val savetestpath = args(3)
    val savecontinuefeaturepath = args(4) //连续特征的离散区间
    val saveidfeaturepath = args(5)    //存储离散特征编号
    val saveselectfeapath = args(6)    //存储最终的特征编号
    val configfile = sc.textFile(configpath).collect().mkString("")
    val xmldata = analysisconfigfile(configfile)
    //1.将原来的数据根据xmldata中的selectmap,去掉不用的训练集特征.

    val inputdata = sc.textFile(inputdatapath)
    val serialclass = new SerialFeature()
        serialclass.setdiscxmldata(xmldata)
        serialclass.settrainfile(savetrainpath)
        serialclass.settestfile(savetestpath)

    inputdata.persist()
    val selectdata = serialclass.selectdata(inputdata)

    selectdata.take(5).map(x => println(x))
    //2.等频离散以及编号.
      serialclass.discret(selectdata) //离散
    val serialdata = serialclass.serial(selectdata) //编号,
     serialclass.savetrainandtest(serialdata) //保存训练集和测试集
    val featuremodel = serialclass.singlefealogloss(serialdata)
    //保存模型文件
    //保存连续特征
    sc.makeRDD(featuremodel.savecontinuefeat()).saveAsTextFile(savecontinuefeaturepath)
    //保存离散特征
    sc.makeRDD(featuremodel.saveidfeat()).saveAsTextFile(saveidfeaturepath)

    //保存,输出数据中,特征的编号以及每个特征的logloss
    var newarr = ArrayBuffer[String]()
    for((k,v) <- xmldata.selectMap){
      if(featuremodel.lossmap.contains(k)){
        newarr += (k+"\t"+v+"\t"+featuremodel.lossmap.get(k).get)
      }else{
        newarr += (k+"\t"+v)
      }
    }
    sc.makeRDD(newarr.toArray).saveAsTextFile(saveselectfeapath)

  }
}