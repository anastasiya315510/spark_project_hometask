package scala_domain

import org.apache.spark.rdd.RDD


import scala.collection.mutable.ListBuffer


case class DomainScala(){
  def domainScala(rdd: RDD[String]): ListBuffer[String] ={
    var res = new ListBuffer[String]()
      rdd.take(40)
      .flatMap(line=> line split(" "))
      .map(line=> line.substring(0,line.lastIndexOf(".")))
      .groupBy(line=> line.substring(0,line.lastIndexOf(".")) )
        .flatMap(s=>{
          if((s._2.size>=2)) res+=s"${s._1}." else res+=s"${s._2.mkString}."
        })
    res
  }


}
