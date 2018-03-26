package com;

import com.aggregate.AvgCount;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class ApplicationRun {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("E:\\workspace\\Kettle-Java-Test\\pom.xml");
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );
        JavaPairRDD<String,Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2(s, 1);
                    }
                }
        ).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });
//        counts.saveAsTextFile("E:\\workspace\\Kettle-Java-Test\\pom_copy.xml");
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map( x -> x*x );
        System.out.println(StringUtils.join(result.collect(), ","));

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        Integer sum = rdd1.reduce( (x, y) -> x + y );
        System.out.println(sum);

        /**
         * spark统计操作 累加操作
         */
        AvgCount initial = new AvgCount(0, 0);
        Function2<AvgCount, Integer, AvgCount> addAndCount = (a, x) -> {
            a.total += x;
            a.num += 1;
            return a;
        };

        /**
         * 合并操作
         */
        Function2<AvgCount, AvgCount, AvgCount> combine = (a, b) -> {
            a.total += b.total;
            a.num += b.num;
            return a;
        };
        AvgCount result1 = rdd.aggregate(initial, addAndCount ,combine);
        System.out.println(result1.avg());
    }
}
