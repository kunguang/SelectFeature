package eval

import org.apache.spark.rdd.RDD

/**
  * Created by zhukunguang on 16/11/25.
  */
class Logloss extends Serializable{



  //输入数据格式,预测ctr,实际label(1/0)

  def getlogloss(data:RDD[(Double,Int)]):Double={
    //
    val lossvalue = data.map{x =>
      var loss = 0.0
      if(x._2 == 1){
        var value = x._1
        if(x._1 < 0.000001){
          value = 0.000001
        }
        loss = -math.log(value)
      }else{
        var x2 = 1.0-x._1
        if(x2 < 0.000001){
          x2 = 0.000001
        }
        loss = -math.log(x2)
      }
      (loss,1.0)
    }.reduce((x,y)=>(x._1+y._1,x._2+y._2))

    lossvalue._1/lossvalue._2

  }

  //我们平时需要计算单个特征的logloss,
  //计算方法,
  // 1.首先计算出该特征下每个特征值的曝光次数和点击次数,featurevalue  pv  clk
  //2.根据推导公式,当样本中出现featurevalue这个特征时,只有当预测该样本点击概率是clk/pv时,整体的Logloss才会最小.

  //1.  //输入数据格式,特征编号或者特证名字,实际label(1/0)
  def groupfeature(data:RDD[(String,Int)]): RDD[(String,(Int,Int))] ={

    data.map{x =>
      if(x._2 == 1){
        (x._1,(1,0))
      }else{
        (x._1,(0,1))
      }
    }.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))

  }

  //2.计算logloss

    def computesinglelogloss(data:RDD[(String,(Int,Int))]): Double ={

      val result = data.map{x =>
        var pos =x._2._1
        var neg = x._2._2
        var ctr = pos/(pos+neg+1.0)
        var res = Tuple2(0.0,1)
        //如果特征下都是正样本
        if(ctr >= 0.999999){
            res = (0.0,pos+neg)
        }else if(ctr <= 0.000001){
         // 如果特征下都是负样本
          res = (0.0,pos+neg)
        }else{
          var value = -pos*math.log(ctr)-neg*math.log(1-ctr)
          res = (value,pos+neg)
        }

        res

      }.reduce((x,y)=>(x._1+y._1,x._2+y._2))
      result._1/result._2
    }

}
