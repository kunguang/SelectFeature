package SelectFeature
import util.control.Breaks._

/**
  * Created by zhukunguang on 16/11/17.
  */
class test {

}


object test{

  def main(args:Array[String]): Unit ={

    var tmpstring = xml.XML.loadString("<item><sex>aaa</sex></item>")

   breakable{
     for(i <- 0 until 2){
       for(j <- 0 until 3){
          if(j == 2){
            break
          }
         println(j)

       }
     }
   }






  }
}