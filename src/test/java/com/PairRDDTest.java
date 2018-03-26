package com;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

public class PairRDDTest {
    @Test
    public void pairRDDTest() {
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputRDD = sc.textFile("E:\\workspace\\Kettle-Java-Test\\pom.xml");
        PairFunction<String, String, String> keyData = ( x ) -> new Tuple2<String, String>(x.split(" ")[0], x);
        JavaPairRDD<String, String> pairs = inputRDD.mapToPair(keyData);
        Function<Tuple2<String, String>, Boolean> longWordFilter = ( keyValue ) -> keyValue._2().length() < 20;
        JavaPairRDD<String, String> result = pairs.filter(longWordFilter);
        JavaRDD<String> words = inputRDD.flatMap( ( x ) -> Arrays.asList(x.split(" ")));
        JavaPairRDD<String, Integer> result1 = words.mapToPair( ( x ) -> new Tuple2(x, 1));
        JavaPairRDD<String, Integer> result2 = result1.reduceByKey( (a, b) -> a + b );

    }
}
