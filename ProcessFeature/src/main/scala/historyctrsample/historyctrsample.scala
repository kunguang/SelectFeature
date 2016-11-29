package historyctrsample

import org.apache.spark.{SparkContext, SparkConf}
import SelectFeature.SelectFeature
/**
  * Created by zhukunguang on 16/11/22.
  */

object historyctrsample{

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setAppName("kunguangadfea")
      .set("spark.yarn.driver.memoryOverhead", "4000").set("spark.akka.frameSize", "128").set("spark.driver.maxResultSize", "3g")


    val sc = new SparkContext(conf)

    val inputdatapath = args(0)
    val featuremapfile = args(1)
    val configpath = args(2)
    val savepath = args(3)
    val configfile = sc.textFile(configpath).collect().mkString("")
    val xmldata = SelectFeature.analysisconfigfile(configfile)

    var readdata = sc.textFile(inputdatapath)
    var ctrclass = new getctrsample
    ctrclass.setxmldata(xmldata)
    val processdata = ctrclass.processdata(readdata)
    val featuremapdata = sc.textFile(featuremapfile)
    ctrclass.getctrmap(featuremapdata)
    val savedata = ctrclass.getfinaldata(processdata)
    savedata.saveAsTextFile(savepath)
  }
}
