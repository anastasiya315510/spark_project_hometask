package nice_service_scala

import org.apache.spark.rdd.RDD

case class NiceServiceScala() {

  def countService(rdd: RDD[String]):  Long ={
  rdd filter(_.length >3) count()


  }


}

