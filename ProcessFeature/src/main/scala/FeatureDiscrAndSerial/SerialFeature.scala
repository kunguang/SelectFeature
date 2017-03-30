package FeatureDiscrAndSerial

import Model.{FeatureModel, discxmldata}
import eval.{BinaryAUC, Logloss}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import util.control.Breaks._

import scala.collection.mutable.HashMap

/**
  * Created by zhukunguang on 16/11/17.
  */
class SerialFeature extends Serializable{



  var xmldata:discxmldata = null

  var trainfile:String = ""
  var testfile:String = ""
  var featuremodel = new FeatureModel()

  def setdiscxmldata(data:discxmldata): Unit ={
    this.xmldata = data
  }

  def settrainfile(file:String): Unit ={
    this.trainfile = file
  }

  def settestfile(file:String): Unit ={
    this.testfile = file
  }

  //将原来的数据根据xmldata中的selectmap,去掉不用的训练集特征.同时,给每个特征值加上自己隶属的编号,
  //比如age_feedid这个特征属性是第7位,有个特征值是12_400001,通过下面的方法就改造成 7:12_400001,为了好识别
  //如果是多个,就改造成 index:value|value|value
  def selectdata(olddata:RDD[String]): RDD[String] ={

      olddata.map{x =>

        var tokens = x.split(",")
        var newdata = new Array[String](xmldata.selectMap.size)
        var tmparr:Array[String] = null
        var newtmparr:Array[String] = null

        for((k,v) <- xmldata.selectMap){
            if(xmldata.singleMap.contains(k)){
                //主要是处理多ID特征
                tmparr = tokens(xmldata.singleMap.get(k).get).split("\\|")
                newtmparr = new Array[String](tmparr.length)
                if(k == "label"){
                  newtmparr(0) = tmparr(0) //label不需要编码特征,所以不需要加前缀特征编号
                }else{
                  for(i <- 0 until tmparr.length){
                    if(xmldata.continuefeatmap.contains(v)){
                      newtmparr(i) = tmparr(i).toDouble +""
                    }else{
                      newtmparr(i) = tmparr(i)

                    }
                  }
                }

              if(k == "label"){
                newdata(v) = tmparr(0)
              }else{
                var tmpvalue = newtmparr.mkString("|")
                if(tmpvalue == ""){
                  tmpvalue = "0"  //如果特征值为空，默认为"0"
                }
                newdata(v) = v+":"+tmpvalue
              }
            }
        }
        newdata.mkString(",")
      }
  }

  def discret(data:RDD[String]): FeatureModel = {

    //连续特征的2个变量
    // key是特征编号 value是这个连续特征的区间值序列,从小到大排序的
    var continuefeaturemap = new collection.mutable.HashMap[Int, ArrayBuffer[Double]]
    //key是 特征编号:特征值  value是这个key的全局编号.特征值来源于上面finalfeaturemap中的value
    var continuefeaturebianhao = new collection.mutable.HashMap[String, Int]

    //ID类特征的编号
    var idfeaturebianhao = new collection.mutable.HashMap[String, Int]

    /*
    整体编号是分为2部分 先编号连续特征的,再编号离散特征的.
     */

    //获取所有的特征

    val collectres = data.flatMap { x =>

      var tokens = x.split(",")

      var array = new ArrayBuffer[(String, Int)]()
      var tmparray: Array[String] = null
      var index = ""
      for (i <- 1 until tokens.length) {
        index = tokens(i).split(":")(0)
        tmparray = tokens(i).split(":")(1).split("\\|")
        for (value <- tmparray) {
          array += Tuple2(index+":"+value, 1)

        }
      }
      array.toTraversable
    }.reduceByKey((x, y) => x + y).filter(x => x._2 >= xmldata.featurefrequent).collect()


    //先对连续特征进行编码
    println("collectressize\t" + collectres.size)
    //首先存储每个连续特征下面都有哪些特征值
    var featuremap = new collection.mutable.HashMap[Int, ArrayBuffer[Double]]
    //存储每个连续特征下有多少条数据,就是样本个数的大小.用来分块用
    var countmap = new collection.mutable.HashMap[Int, Double]

    var tokens: Array[String] = null
    //存储每个特征出现的次数,用来分块用
    var datavaluemap = new collection.mutable.HashMap[String, Int]
    for (value <- collectres) {
      //循环内的break表示continue
      breakable {
        tokens = value._1.split(":")

        //如果不是连续特征,就继续
        if (!xmldata.continuefeatmap.contains(tokens(0).toInt)) {
          break()
        }

        datavaluemap += (value._1 -> value._2)

        if (!featuremap.contains(tokens(0).toInt)) {
          featuremap += (tokens(0).toInt -> new ArrayBuffer[Double]())
        }
        featuremap(tokens(0).toInt) += tokens(1).toDouble

        if (!countmap.contains(tokens(0).toInt)) {
          countmap += (tokens(0).toInt -> 0.0)
        }
        countmap(tokens(0).toInt) = countmap(tokens(0).toInt) + value._2
      }

    }

    for ((k, v) <- featuremap) {
      featuremap(k) = v.sorted
    }

    for ((k, v) <- countmap) {
      countmap(k) = countmap(k) / (xmldata.continuefeatmap.get(k).get) //总数除以块的个数,即每个区间内大概有多少条数据
      // println("k\t"+countmap(k))
    }

    dengpinlisan(featuremap, countmap, continuefeaturemap, continuefeaturebianhao, datavaluemap)

  //编号从1开始
    var startbianhao = continuefeaturebianhao.size
    if(startbianhao == 0){
      startbianhao = 1
    }
    //获取离散ID类特征编号
    for (value <- collectres) {
      //循环内的break表示continue
      breakable {
        tokens = value._1.split(":")

        //如果是连续特征,就继续
        if (xmldata.continuefeatmap.contains(tokens(0).toInt)) {
          break()
        }


        idfeaturebianhao.put(value._1, startbianhao)
        startbianhao = startbianhao + 1
      }

    }
    featuremodel.setcontinuefeaturebianhao(continuefeaturebianhao)
    featuremodel.setcontinuefeaturemap(continuefeaturemap)
    featuremodel.setidfeaturebianhao(idfeaturebianhao)
    featuremodel
  }

    def serial(data:RDD[String]):RDD[(String,Double)]= {
      //因为前面对特征做了过滤,所以接下来在映射样本的时候必然会造成某些特征缺失.统一用startbianhao标记

      //利用上面的编号 将训练集改成libsvm格式
      var startbianhao = featuremodel.continuefeaturebianhao.size + featuremodel.idfeaturebianhao.size

      val alldata = data.map { x =>

        var tokens = x.split(",")
        var res = tokens(0).toDouble + "" //label
      var values: Array[String] = null
        var ctr = 0.0
        var index = 0
        var isqueshi = 0 //用来表示该样本所有的特征是否都找到了编号,连续特征一般没有缺失
      var tmpres = ""
        var junzhi: Double = 0.0
        var tmparr: Array[String] = null
        var tmptokens:Array[String] = null

        for (i <- 1 until tokens.length) {
          tmpres = ""
          values = tokens(i).split(":")
          index = values(0).toInt
          //1.如果是连续特征,就用连续特征编号.
          if (featuremodel.continuefeaturemap.contains(index)) {
        //    ctr = values(1).toFloat
            tmparr = values(1).split("\\|")
            junzhi = (100.0 / tmparr.length).toInt / 100.0 //针对多ID类特征,用户兴趣,这些特征的x共用1.来源于svd均值,只保留小数点后2位

            for(value <- tmparr){
              ctr = value.toDouble
              breakable {
                for (j <- 0 until featuremodel.continuefeaturemap(index).size) {

                  if (ctr <= featuremodel.continuefeaturemap(index)(j)) {
                    if (j == 0) {
                      tmpres = tmpres + "," + featuremodel.continuefeaturebianhao(index + ":" + featuremodel.continuefeaturemap(index)(j)) + ":" + junzhi

                    } else {
                      if ((featuremodel.continuefeaturemap(index)(j) - ctr) > (ctr - featuremodel.continuefeaturemap(index)(j - 1))) {
                        tmpres = tmpres + "," + featuremodel.continuefeaturebianhao(index + ":" + featuremodel.continuefeaturemap(index)(j - 1)) + ":" + junzhi

                      } else {
                        tmpres = tmpres + "," + featuremodel.continuefeaturebianhao(index + ":" + featuremodel.continuefeaturemap(index)(j)) + ":" + junzhi

                      }
                    }
                    break()
                  }
                }

              }
            }

          } else {
            tmparr = values(1).split("\\|")
            junzhi = (100.0 / tmparr.length).toInt / 100.0 //针对多ID类特征,用户兴趣,这些特征的x共用1.来源于svd均值,只保留小数点后2位
            for (value <- tmparr) {
              if (featuremodel.idfeaturebianhao.contains(index+":"+value)) {
                tmpres = tmpres + "," + featuremodel.idfeaturebianhao.get(index+":"+value).get + ":" + junzhi
              } else {
                isqueshi = 1
                tmpres = tmpres + "," + startbianhao + ":" + junzhi
              }
            }
          }
          res = res + " " + tmpres.stripPrefix(",")

        }

        if (isqueshi == 1 && xmldata.isdiscardsample == 1) {
          res = ""
        }
        var random = new Random()
        (res, random.nextDouble())
      }.filter(x => x._1 != "")
      alldata.persist()

      alldata
    }

    def savetrainandtest(alldata:RDD[(String,Double)]): Unit ={
      //产生训练集和测试集
      alldata.filter(x => x._2 >= 0.1).map{x=>
          var tokens = x._1.split(" ")
          var tmptokens:Array[String] = null
          var res = tokens(0)
          for(i <- 1 until tokens.length){
                tmptokens = tokens(i).split(",")
                for(value <- tmptokens){
                  res = res + " " + value
                }
          }
        res
      }.saveAsTextFile(trainfile)

      alldata.filter(x => x._2 < 0.1).map{x=>
        var tokens = x._1.split(" ")
        var tmptokens:Array[String] = null
        var res = tokens(0)
        for(i <- 1 until tokens.length){
          tmptokens = tokens(i).split(",")
          for(value <- tmptokens){
            res = res + " " + value
          }
        }
        res
      }.saveAsTextFile(testfile)
    }

  //计算单特征

  def singlefealogloss(alldata:RDD[(String,Double)]): FeatureModel ={

    var lossandauc = new mutable.HashMap[String,String]()
    var lossclass = new Logloss()
    var auc = new BinaryAUC()
    for((k,v) <- xmldata.selectMap){
      // 0 是label
      if(v != 0){
        val curdata = alldata.map{x =>
            var tokens = x._1.split(" ")
          val label = tokens(0).toDouble.toInt
          var featurename = tokens(v)
          (featurename,label)
        }
        val groupdata = lossclass.groupfeature(curdata)
        groupdata.persist()
        val logloss = lossclass.computesinglelogloss(groupdata)

        //compute auc
        /*
          1.计算出每个ctr对应的正样本次数和负样本次数,ctr保留6位
          2.调用接口.
         */
        val grouprankdata = groupdata.map{x =>
          ((100000.0*x._2._1/(x._2._2+x._2._1+1.0)).toInt/100000.0,(x._2._1,x._2._2))
        }.reduceByKey((x,y) => (x._1+y._1,x._2+y._2)).collect()
        //已经把ctr降低到10W个量级,单机排序就可以了
        //如果是分布式.上一步就不用collect,直接用分布式auc计算.
        val aucvalue = auc.singlecomputeauc(grouprankdata)
        lossandauc.put(k,logloss+"\t"+aucvalue)


      }
    }
    featuremodel.setlossmap(lossandauc)
    featuremodel

  }



  //最普通的等频离散
  def dengpinlisan(featuremap:HashMap[Int,ArrayBuffer[Double]], countmap:HashMap[Int,Double],
                   continuefeaturemap:HashMap[Int,ArrayBuffer[Double]],continuefeaturebianhao:HashMap[String,Int],
                   datavaluemap:HashMap[String,Int]): Unit ={

    var sum = 0.0

    var initbianhao = 1
    for((k,v) <- featuremap){
      sum = 0.0
      continuefeaturemap += (k -> new ArrayBuffer[Double]())
      for(i <- 0 until v.size){
        sum = sum + datavaluemap(k+":"+v(i))
        if(sum >= countmap(k)){
          continuefeaturemap(k) += v(i)
          continuefeaturebianhao += (k+":"+v(i) -> initbianhao)
          initbianhao = initbianhao + 1
          sum = 0.0

        }

      }


      continuefeaturemap(k) += Double.MaxValue
      continuefeaturebianhao += (k+":"+Double.MaxValue -> initbianhao)
      initbianhao = initbianhao + 1
    }

  }


//当所有的特征都是连续特征时 使用等频离散的方法

  def processdata(data:RDD[String]): FeatureModel ={
    val collectres = data.flatMap{x =>
      var tokens = x.split(",")
      var array = new ArrayBuffer[(String,Int)]()
      for(i <- 1 until tokens.length){
        array += Tuple2(tokens(i),1)
      }
      array.toTraversable
    }.reduceByKey((x,y) => x+y).collect()

    println("collectressize\t"+collectres.size)
    var featuremap = new collection.mutable.HashMap[Int,ArrayBuffer[Double]]
    var countmap = new collection.mutable.HashMap[Int,Double]
    var tokens:Array[String] = null
    var datavaluemap = new collection.mutable.HashMap[String,Int]
    for(value <- collectres){
      datavaluemap += (value._1 -> value._2)
      tokens = value._1.split(":")
      if (!featuremap.contains(tokens(0).toInt)){
        featuremap += (tokens(0).toInt -> new ArrayBuffer[Double]())
      }
      featuremap(tokens(0).toInt) += tokens(1).toInt

      if(!countmap.contains(tokens(0).toInt)){
        countmap += (tokens(0).toInt -> 0.0)
      }
      countmap(tokens(0).toInt) = countmap(tokens(0).toInt) + value._2
    }

    for((k,v) <- featuremap){
      featuremap(k) = v.sorted
    }

    for((k,v) <- countmap){
      countmap(k) = 5000.0 //块大小
      println("k\t"+countmap(k))
    }

    var finalfeaturemap = new collection.mutable.HashMap[Int,ArrayBuffer[Double]]
    var featuremapbianhao = new collection.mutable.HashMap[String,Int]
    dengpinlisan(featuremap,countmap,finalfeaturemap,featuremapbianhao,datavaluemap)

    println("bianhao")
    var initbianhao = featuremapbianhao.size;


    println(initbianhao)
    val alldata = data.map{x =>

      var tokens = x.split("\t")
      var res = tokens(0)
      var values : Array[String] = null
      var ctr = 0.0
      var index = 0

      for(i <- 1 until tokens.length){
        values = tokens(i).split(":")
        index = values(0).toInt
        ctr = values(1).toInt
        breakable{
          for(j <- 0 until finalfeaturemap(index).size){

            if(ctr <= finalfeaturemap(index)(j)){
              if(j == 0){
                res = res + " " + featuremapbianhao(index+":"+finalfeaturemap(index)(j))+":" + 1

              }else{
                if((finalfeaturemap(index)(j) - ctr) > (ctr - finalfeaturemap(index)(j-1))){
                  res = res + " " + featuremapbianhao(index+":"+finalfeaturemap(index)(j-1))+":" + 1

                }else{
                  res = res + " " + featuremapbianhao(index+":"+finalfeaturemap(index)(j))+":" + 1

                }
              }
              break()
            }
          }

        }
      }
      var random = new Random()
      (res,random.nextDouble())
    }

    alldata.filter(x => x._2 >= 0.1).map(x=>x._1 ).saveAsTextFile(this.trainfile)
    alldata.filter(x => x._2 < 0.1).map(x=>x._1).saveAsTextFile(this.testfile)

    var featuremodel = new FeatureModel
    featuremodel.setcontinuefeaturebianhao(featuremapbianhao)
    featuremodel.setcontinuefeaturemap(featuremap);


    return featuremodel

  }



}
