package nice_service_scala;

import fridays_13_java.Fridays_13_Scala;
import most_popular_word_scala.PopularWordScala;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;
import scala_domain.DomainScala;

import java.util.List;

import static org.junit.Assert.*;

public class NiceServiceScalaTest {
    private  static SparkConf conf;
    private static JavaSparkContext sparkContext;
    private NiceServiceScala niceService;
    private Fridays_13_Scala fridays13;
    private PopularWordScala word;
    private DomainScala scalaDom;

    @BeforeClass
    public static void setUpForClass() throws Exception {
        conf = new SparkConf().setAppName("hello").setMaster("local[*]");
        sparkContext = new JavaSparkContext(conf);

    }

    @Before
    public void setUp() throws Exception {
        fridays13 = new Fridays_13_Scala();
        niceService = new NiceServiceScala();
        word = new PopularWordScala();
        scalaDom = new DomainScala();
    }

    @Test
    public void countService() {
        RDD<String> rdd = sparkContext.parallelize(List.of("java", "scala", "python", "C", "c++")).rdd();
        assert(niceService.countService(rdd)== 3L);
    }

    @Test
    public  void countFridays() {
        List<Tuple2<String, String>> apply = List.of(Tuple2.apply("2020", "2020"));
        RDD<Tuple2<String, String>> rdd = sparkContext.parallelize(apply).rdd();
        Assert.assertEquals(fridays13.countFridays(rdd).length(), 1 );
        Assert.assertEquals(fridays13.countFridays(rdd).apply(0)._2(), 2 );


    }
    @Test
    public void popularWordScala() {
        RDD<String> rdd = sparkContext.textFile("src/main/resources/my.txt").rdd();
        Object scala = word.popularWordScala(rdd);
        Assert.assertEquals(scala, Tuple2.apply("word", 5));

    }

    @Test
    public void domainScala(){
        RDD<String> rdd = sparkContext.textFile("src/main/resources/domain.txt").rdd();
        scala.collection.immutable.List<String> stringList = scalaDom.domainScala(rdd).toList();

        List<String> list = List.of("tuoi.hml.com.", "dom.xvn.", "dom.hml.com.");
        Assert.assertTrue(stringList.contains(list.get(0)));
        Assert.assertTrue(stringList.contains(list.get(1)));
        Assert.assertTrue(stringList.contains(list.get(2)));
        Assert.assertTrue(stringList.size()== list.size());


    }
}