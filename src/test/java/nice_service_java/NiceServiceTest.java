package nice_service_java;

import fridays_13_java.CounterFridays13;
import most_popular_word_in_the_text_java.PopularWord;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class NiceServiceTest {
    private  static SparkConf conf;
    private static JavaSparkContext javaSparkContext;
    private NiceService niceService;
    private CounterFridays13 fridays13;
    private PopularWord popularWord;


    @BeforeClass
    public static void setUpForClass() throws Exception {
        conf = new SparkConf().setAppName("hello").setMaster("local[*]");
        javaSparkContext = new JavaSparkContext(conf);

    }

    @Before
    public void setUp() throws Exception {
        niceService = new NiceService();
        fridays13 = new CounterFridays13();
        popularWord = new PopularWord();

    }

    @Test
    public void countSomething(){
        JavaRDD<String> rdd = (JavaRDD<String>) javaSparkContext.parallelize(
                Stream.of(
                        "java", "scala", "python", "C", "c++"
                ).collect(Collectors.toList()));
        long countSomething = niceService.countSomething(rdd);
        Assert.assertEquals(3, countSomething);
    }

    @Test
    public void count(){
        List<Tuple2< Integer,Integer>> expectedInput = List.of(Tuple2.apply(2020, 2020));
        JavaPairRDD< Integer, Integer> expectedRDD = javaSparkContext.parallelizePairs(expectedInput);
        List<Map.Entry<Integer, Long>> collect = fridays13.count(expectedRDD).collect(Collectors.toList());
        Assert.assertEquals(collect.get(0).getValue().intValue(), 2);
    }

    @Test
    public void popularWord(){
        JavaRDD<String> rdd = javaSparkContext.textFile("src/main/resources/my.txt");
        Map.Entry<String, Long> string= popularWord.popularWord(rdd);
        Assert.assertEquals(string.getValue().longValue(), 5L );
        Assert.assertEquals(string.getKey(), "word" );

    }





}