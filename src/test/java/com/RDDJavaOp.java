package com;

import com.aggregate.AvgCount;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;

import java.util.Arrays;

public class RDDJavaOp {
    @Test
    public void filterRddTest() {
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputRDD = sc.textFile("E:\\workspace\\Kettle-Java-Test\\pom.xml");

        /**
         * 获取含有error字段的字符串行
         */
        JavaRDD<String> errorsRDD = inputRDD.filter(s -> s.contains("error"));

        /**
         * 获取含有warn字段的字符串行
         */
        JavaRDD<String> warnsRDD = inputRDD.filter(
                s -> s.contains("warning")
        );

        /**
         * 合并记录
         */
        JavaRDD<String> badLinesRDD = errorsRDD.union(warnsRDD);

        /**
         * 基础统计
         */
        System.out.println("Input had" + badLinesRDD.count() + " concerning lines");
        System.out.println("Here are 10 examples");

        /**
         * 获取前10行数据
         */
        for(String line : badLinesRDD.take(10)) {
            System.out.println(line);
        }

        /**
         *  输出结果为 1,4,9,16
         */
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map( x -> x*x );
        System.out.println(StringUtils.join(result.collect(), ","));

        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi"));
        JavaRDD<String> words = lines.flatMap( s -> Arrays.asList(s.split(" ")));
        words.first();

        Integer sum = rdd.reduce( (x, y) -> x + y );
        System.out.println(sum);

        /**
         * spark统计操作 累加操作 Integer是集合中的值 也就是x
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
