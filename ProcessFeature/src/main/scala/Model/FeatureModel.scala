package Model

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
/**
  * Created by zhukunguang on 16/11/17.
  */
class FeatureModel extends Serializable{


  //连续特征的2个变量
  // key是特征编号 value是这个连续特征的区间值序列,从小到大排序的
  var continuefeaturemap = new collection.mutable.HashMap[Int,ArrayBuffer[Double]]
  //key是 特征编号:特征值  value是这个key的全局编号.特征值来源于上面finalfeaturemap中的value
  var continuefeaturebianhao = new collection.mutable.HashMap[String,Int]

  //ID类特征的编号
  var idfeaturebianhao = new collection.mutable.HashMap[String,Int]

  var lossmap = new mutable.HashMap[String,String]() //单特征logloss

  def setcontinuefeaturemap(map:HashMap[Int,ArrayBuffer[Double]]): Unit ={
        this.continuefeaturemap = map
  }

  def setcontinuefeaturebianhao(map:HashMap[String,Int]): Unit ={
      this.continuefeaturebianhao = map
  }

  def setidfeaturebianhao(map:HashMap[String,Int]): Unit ={
      this.idfeaturebianhao = map
  }

  def savecontinuefeat(): ArrayBuffer[String] ={
    var feaarr = new ArrayBuffer[String]()

    for((k,v) <- continuefeaturemap){
      var res = k+""

      for(i <- 0 until v.size){
        res = res + "," + v(i)+":"+continuefeaturebianhao(k+":"+v(i))
      }
      feaarr += res
    }
    feaarr
  }

  def saveidfeat(): ArrayBuffer[String] ={
    var feaarr = new ArrayBuffer[String]()

    for((k,v) <- idfeaturebianhao){
      var res = k+","+v

      feaarr += res
    }
    feaarr
  }

  def setlossmap(map:mutable.HashMap[String,String]): Unit ={
    this.lossmap = map
  }
}
