package fridays_13_java;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class CounterFridays13 {
    public Stream<Map.Entry<Integer, Long>> count(JavaPairRDD<Integer, Integer> lines){
        LocalDate dateStart = LocalDate.of(lines.rdd().first()._1, 1, 13);
        LocalDate dateEnd = LocalDate.of(lines.rdd().first()._2, 12, 14);
       return Stream.iterate(dateStart, date -> date.plusMonths(1))
                .limit(ChronoUnit.MONTHS.between(dateStart, dateEnd))
                .filter(date -> date.getDayOfWeek() == DayOfWeek.FRIDAY)
                .filter(date -> date.getDayOfMonth() == 13)
                .collect(groupingBy(LocalDate::getYear, counting()))
                .entrySet().stream()
                .sorted(Map.Entry.comparingByValue());


    }


}
