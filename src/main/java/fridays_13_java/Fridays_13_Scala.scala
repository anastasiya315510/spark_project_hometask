package fridays_13_java

import org.apache.spark.rdd.RDD

import java.time.{DayOfWeek, LocalDate}
import scala.Int.unbox



case class Fridays_13_Scala(){

  def countFridays(rdd: RDD[(String, String)]):List[(Int, Int)]={
  List.range(rdd.first()._2.toInt, rdd.first()._2.toInt+1)
      .map(year=>(year, (1 to 12).count(LocalDate.of(year, _, 13)
      .getDayOfWeek eq DayOfWeek.FRIDAY))).sortBy(-_._2)
  }

}
