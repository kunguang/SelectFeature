package Model

/**
  * Created by zhukunguang on 16/11/17.
  */
class selectxmldata extends Serializable{


  var singleMap:Map[String,Int] = null //KEY是name,VALUE是index
  var crossArray:Array[String] = null//存储category是crossfeature的name,name_name_name  用_链接的
  var selectMap:Map[String,Int] = null//包含selectindex的数据,Key是Name,value是selectIndex
  var featurefrequent:Int = 0 //特征过滤阈值
  var replaceMap:Map[String,Array[String]] = null //当某个特征缺失时,用另外一个特征的值代替.用在getctrsample中
  var congtinuemapname:Array[String] = null //需要历史ctr映射的特征名字

  //存放一些临时变量,针对replace.比如当feedid的ctr缺失,我想用custuid的ctr替换,但是custuid的历史ctr又不在最终的特征中.
  // 所以custuid就是临时变量
  var dropindex:Array[Int] = null

  def setSinglemap(map:Map[String,Int]): Unit ={
    this.singleMap = map
  }

  def setCrossArray(array:Array[String]): Unit ={
    this.crossArray = array
  }

  def setSelectmap(map:Map[String,Int]): Unit ={
    this.selectMap = map
  }

  def setreplacemap(map:Map[String,Array[String]]): Unit ={
    this.replaceMap = map
  }

  def setfeaturefrequent(yuzhi:Int): Unit ={
    this.featurefrequent = yuzhi
  }

  def setcongtinuemapname(array:Array[String]): Unit ={
    this.congtinuemapname = array
  }

  def setdropindex(array:Array[Int]): Unit ={
    this.dropindex = array;
  }
}
