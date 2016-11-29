package Model

/**
  * Created by zhukunguang on 16/11/22.
  */
class featurectrxmldata extends Serializable{


  var singleMap:Map[String,Int] = null //KEY是name,VALUE是index
  var crossArray:Array[String] = null //存储category是crossfeature的name,name_name_name  用_链接的
  var selectMap:Map[String,Int] = null //包含selectindex的数据,Key是Name,value是selectIndex

  var partitionname = "" //分区字段

  def setSinglemap(map:Map[String,Int]): Unit ={
    this.singleMap = map
  }

  def setCrossArray(array:Array[String]): Unit ={
    this.crossArray = array
  }

  def setSelectmap(map:Map[String,Int]): Unit ={
    this.selectMap = map
  }

  def setPartitionname(name:String): Unit ={
    this.partitionname = name;
  }
}
