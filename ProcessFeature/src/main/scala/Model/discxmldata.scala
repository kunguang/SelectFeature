package Model

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhukunguang on 16/11/17.
  */
class discxmldata extends Serializable {

  var singleMap:Map[String,Int] = null //KEY是name,VALUE是index
  var selectMap:Map[String,Int] = null //包含那些带select属性的name,index重新编号,label会认为排在第1位
  var continuefeatmap:Map[Int,Int] = null //连续特征,需要等频离散.key是selectMap中重新变过后的index,value是这个特征分为多少块
  var featurefrequent:Int = 0 //特征过滤阈值
  var isdiscardsample:Int = 0 //是否丢失样本,因为如果特征过滤了,必然在映射样本时会丢失一些特征,丢失的特征用默认编号编码.如果是1,就丢弃,否则保留.
  def setSinglemap(map:Map[String,Int]): Unit ={
    this.singleMap = map
  }

  def setdiscardsample(v:Int): Unit ={
    this.isdiscardsample = v
  }

  def setSelectmap(map:Map[String,Int]): Unit ={
    this.selectMap = map
  }

  def setContinuemap(map:Map[Int,Int]): Unit ={
    this.continuefeatmap = map
  }

  def setfeaturefrequent(yuzhi:Int): Unit ={
    this.featurefrequent = yuzhi
  }

  def saveselectmap(): ArrayBuffer[String] ={

    var newarr = ArrayBuffer[String]()

    for((k,v) <- selectMap){

      newarr += (k+"\t"+v)
    }
    newarr
  }
}


