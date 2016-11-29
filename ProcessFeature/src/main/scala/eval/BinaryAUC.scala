package eval



/**
  * Created by zhukunguang on 16/11/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._
import scala.collection.{mutable, Iterator}
import Array._

class BinaryAUC extends Serializable {
  //输入格式 预测值,label(0/1).


  def grouprankscore(data:RDD[(Double,Int)]): RDD[(Double,(Int,Int))] ={

    data.map{x =>
      if(x._2 == 1){
        (x._1,(1,0))
      }else{
        (x._1,(0,1))
      }
    }.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))

  }

  def getmaxvaluefunc(index:Int,iter:Iterator[(Double,(Int,Int))]) : Iterator[(Double,(Int,Int))]={
    var res = List[(Double,(Int,Int))]()

    var sumcount = 0
    var maxnum = 0.0
    var nextvalue:(Double,(Int,Int)) = null
    while(iter.hasNext){
      nextvalue = iter.next()
      sumcount = sumcount + nextvalue._2._1 + nextvalue._2._2
      if(nextvalue._1 > maxnum){
        maxnum = nextvalue._1
      }
    }
    res.::(maxnum,(index,sumcount))

    res.iterator
  }



  //分布式auc
  //输入数据, (ctr,(pos,num))
  def computeauc(data:RDD[(Double,(Int,Int))]): Double ={

      val sortdata = data.sortByKey()

      /*
          1.计算每个partitions中的最大值以及数据的个数
          2.计算每个partion中的起始排序位置
          3.再在每个partions中计算值
          4.汇总结果  所有pos样本的rank位置相加
       */

    //1.

        val getmaxvaluedata = sortdata.mapPartitionsWithIndex[(Double,(Int,Int))](getmaxvaluefunc).collect()
        val sortmaxnum = getmaxvaluedata.sortWith(_._1 < _._1)

        //key是mappartitionindex,value是这个partition下的起始位置
        var index_startmap = new mutable.HashMap[Int,Int]()
        var sumcount = 1
        for(x <- sortmaxnum){
          index_startmap.put(x._2._1,sumcount)
          sumcount = sumcount + x._2._2
        }

        val everyparvalue = sortdata.mapPartitionsWithIndex{(index,values) =>
          var res = List[(Int,Int,Double)]()

          var poscount = 0
          var negcount = 0
          var totalvalue = 0.0
          var curcount = index_startmap.get(index).get
          var nextvalue:(Double,(Int,Int)) = null
          var totalcount = 0.0
          var pingjun = 0.0
          while(values.hasNext){
              nextvalue = values.next()
              totalcount = nextvalue._2._1 +nextvalue._2._2
              poscount = poscount + nextvalue._2._1
              negcount = negcount + nextvalue._2._2
              pingjun = (curcount + totalcount+curcount-1)/2 //取均值
              totalvalue = totalvalue + pingjun *nextvalue._2._1
              curcount = curcount + totalcount.toInt
          }

          res.::(poscount,negcount,totalvalue)
          res.iterator
        }

        var finalcollect = everyparvalue.reduce((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3))

        var totalpos = finalcollect._1
        var totalneg = finalcollect._2
        var totalsumrank = finalcollect._3

        var auc = (totalsumrank-(totalpos/2.0)*(totalpos+1.0))/((totalpos+0.0)*totalneg)
        auc
  }

  //单机auc
  def singlecomputeauc(data:Array[(Double,(Int,Int))]): Double ={

   // val sortdata = data.sortByKey()

    /*
        1.计算每个partitions中的最大值以及数据的个数
        2.计算每个partion中的起始排序位置
        3.再在每个partions中计算值
        4.汇总结果  所有pos样本的rank位置相加
     */

    //1.

    val sortmaxnum = data.sortWith(_._1 < _._1)


    var poscount = 0
    var negcount = 0
    var totalvalue = 0.0
    var curcount = 0
    var totalcount = 0.0
    var pingjun = 0.0
    for(x <- sortmaxnum){
      totalcount = x._2._1 +x._2._2
      poscount = poscount + x._2._1
      negcount = negcount + x._2._2
      pingjun = (curcount + totalcount+curcount-1)/2 //取均值
      totalvalue = totalvalue + pingjun *x._2._1
      curcount = curcount + totalcount.toInt
    }


    var auc = (totalvalue-(poscount/2.0)*(poscount+1.0))/((poscount+0.0)*negcount)
    auc
  }
}