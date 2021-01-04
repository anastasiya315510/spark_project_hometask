package most_popular_word_scala

import org.apache.spark.rdd.RDD

case class PopularWordScala(){
  def popularWordScala(rdd: RDD[String]):Any ={
    rdd.take(20)
      .toStream
      .map(_.replaceAll(","," "))
      .flatMap(_.split(" ").toStream)
      .groupBy(identity)
      .mapValues(_.size)
      .maxBy(_._2)

  }
}
