package most_popular_word_in_the_text_java;



import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class PopularWord {
    public Map.Entry<String, Long>   popularWord(JavaRDD<String> rdd){
        Map<String, Long> collect = rdd.countByValue().keySet().stream()
                .flatMap(s -> Stream.of(s.replaceAll(","," ").split(" ")))
                .collect(Collectors.groupingBy(word -> word, Collectors.counting()));
        return collect.entrySet()
                .stream().sorted((e,e2)-> (int) (e2.getValue()-e.getValue()))
                .findFirst()
                .orElseThrow();




    }
}
